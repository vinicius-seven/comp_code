"""
Script de consolidação de séries históricas para o banco Itaú.

Este script é inspirado nos scripts anteriores usados para Bradesco,
Santander e Banco do Brasil. Ele percorre todos os arquivos Excel
encontrados na pasta ``input/itau/series_historicas`` relativa a um
``base_path`` definido pelo usuário. Cada arquivo contém várias
planilhas (abas), mas somente as planilhas listadas em ``sheet_targets``
serão processadas. O algoritmo tenta identificar automaticamente a
linha de cabeçalho onde se encontram as datas (ou rótulos de trimestre)
e a coluna a partir da qual começam as datas. As colunas anteriores a
esse ponto são combinadas para formar o nome do atributo. Caso o mesmo
nome de atributo apareça em linhas diferentes da mesma aba, ele é
numerado (#1, #2, etc.). Vários valores de datas dentro da mesma
linha são tratados como série temporal e **não** geram numeradores.

Os rótulos de data (``data_base``) são normalizados para o formato
``YYYY-MM-01``. São aceitos valores como ``Mar25``, ``Mar/25``,
``1T25``, ``30/06/2025`` ou objetos ``datetime``; a função converte
para o mês de encerramento do trimestre correspondente. Por exemplo,
``30/06/2025`` e ``2T25`` tornam-se ``2025-06-01``.

A data de divulgação (``data_divulgacao``) é extraída do nome do
arquivo a partir do padrão ``([1-4])T(\d{2})``; o trimestre determina
o mês de encerramento (março para 1T, junho para 2T, setembro para 3T
e dezembro para 4T). Esse valor é duplicado na saída para atender à
especificação que exige a coluna ``data_divulgacao`` duas vezes.

A saída final é um único CSV consolidado por banco salvo em
``refined/itau/series_historicas/data_ext=YYYY-MM-DD/itau_series.csv``.
O ``YYYY-MM-DD`` corresponde à data de execução do script. Se o script
for rodado mais de uma vez no mesmo dia, o arquivo será sobrescrito.

Para utilizar o script em outro ambiente (por exemplo, AWS Glue),
basta ajustar a variável ``base_path`` para apontar para o local onde
estão os dados de entrada e onde deseja-se gravar os dados de saída.
Nenhuma outra alteração de código é necessária.
"""

import os
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Any

import numpy as np  # type: ignore
import pandas as pd  # type: ignore

# ---------------------------------------------------------------------------
# Configuração de caminho base
# Ajuste esta variável conforme o ambiente de execução. Para execução local,
# utilize um caminho de diretório do sistema de arquivos (ex.: r"C:\\Users\\meu_usuario\\projeto").
# Para execução no AWS Glue, utilize um caminho no S3 (ex.: "s3://meu-bucket/projeto").
# O restante do script constrói caminhos relativos a partir de ``base_path``.
base_path = r"C:\\Users\\BalerionRider\\Documents\\repo\\base_credito"


def normalize_name(name: str) -> str:
    """Normaliza nomes para comparação robusta.

    Remove acentos, converte para minúsculas, remove espaços extras e
    hifens. Retorna a string normalizada.
    """
    nfkd = unicodedata.normalize("NFKD", name)
    without_accents = "".join(c for c in nfkd if not unicodedata.combining(c))
    cleaned = re.sub(r"[\s\-]+", "", without_accents).lower()
    return cleaned


def extract_data_divulgacao(file_name: str) -> str:
    """Extrai a data de divulgação do nome do arquivo.

    Acha o padrão ``([1-4])T(\d{2})`` (insensível a maiúsculas) e converte para
    ``YYYY-MM-01`` de acordo com o trimestre: 1T -> março, 2T -> junho,
    3T -> setembro, 4T -> dezembro. Se não encontrar o padrão, retorna
    string vazia.
    """
    match = re.search(r"([1-4])T(\d{2})", file_name, flags=re.IGNORECASE)
    if not match:
        return ""
    trimestre = int(match.group(1))
    ano = int(match.group(2))
    ano_full = 2000 + ano
    mes_map = {1: 3, 2: 6, 3: 9, 4: 12}
    mes = mes_map.get(trimestre, 1)
    return f"{ano_full:04d}-{mes:02d}-01"


def format_data_base(label: Any) -> str:
    """Formata o rótulo de data para o padrão YYYY-MM-01.

    Aceita formatos como 'Mar/25', 'Jun25', '1T25', datas do tipo
    '30/06/2025' ou objetos datetime/pandas.Timestamp. Converte para o
    primeiro dia do mês de encerramento do trimestre. Se não conseguir
    interpretar, retorna o valor original convertido para string.
    """
    # Se for um objeto Timestamp ou datetime, converte diretamente
    if isinstance(label, (pd.Timestamp, datetime)):
        dt = pd.to_datetime(label)
        year = dt.year
        month = dt.month
        # determina o mês de término do trimestre (3, 6, 9, 12)
        month_end = ((month - 1) // 3 + 1) * 3
        return f"{year:04d}-{month_end:02d}-01"
    if isinstance(label, str):
        text = label.strip()
        # padrão de trimestre, ex: 2T25
        m = re.match(r"([1-4])T(\d{2})", text, flags=re.IGNORECASE)
        if m:
            trimestre = int(m.group(1))
            ano = int(m.group(2))
            ano_full = 2000 + ano
            mes_map = {1: 3, 2: 6, 3: 9, 4: 12}
            mes = mes_map.get(trimestre, 1)
            return f"{ano_full:04d}-{mes:02d}-01"
        # mês abreviado, ex: Mar/25 ou Mar25
        m2 = re.match(r"([A-Za-zÀ-ÿ]{3})/?(\d{2})", text, flags=re.IGNORECASE)
        if m2:
            mes_str = m2.group(1).lower()
            ano = int(m2.group(2))
            ano_full = 2000 + ano
            mes_map_dict = {
                'jan': 1, 'fev': 2, 'feb': 2, 'mar': 3,
                'abr': 4, 'apr': 4, 'mai': 5, 'may': 5,
                'jun': 6, 'jul': 7, 'ago': 8,
                'set': 9, 'sep': 9,
                'out': 10, 'oct': 10,
                'nov': 11, 'dez': 12, 'dec': 12,
            }
            mes_num = mes_map_dict.get(mes_str[:3], None)
            if mes_num is not None:
                trimestre_end_map = {
                    1: 3, 2: 3, 3: 3,
                    4: 6, 5: 6, 6: 6,
                    7: 9, 8: 9, 9: 9,
                    10: 12, 11: 12, 12: 12,
                }
                mes_trimestre = trimestre_end_map.get(mes_num, mes_num)
                return f"{ano_full:04d}-{mes_trimestre:02d}-01"
        # data no formato dd/mm/aaaa ou dd/mm/aa (possivelmente com horário)
        try:
            dt = pd.to_datetime(text, dayfirst=True, errors='raise')
            year = dt.year
            month = dt.month
            month_end = ((month - 1) // 3 + 1) * 3
            return f"{year:04d}-{month_end:02d}-01"
        except Exception:
            pass
    # por padrão, retorna string convertida
    return str(label)


def guess_header_row(df: pd.DataFrame) -> Tuple[int, int]:
    """Identifica a linha de cabeçalho e a coluna inicial de datas.

    Procura uma linha contendo rótulos de datas ou trimestres. São
    aceitos padrões como ``Mar/17``, ``Jun25``, ``2T25``, bem como
    datas no formato ``dd/mm/yy`` ou ``dd/mm/yyyy``. Tenta primeiro
    encontrar uma linha com pelo menos dois rótulos; se não encontrar,
    aceita linhas com um rótulo. A coluna inicial é determinada pelo
    primeiro rótulo encontrado. Se nada for identificado, procura a
    primeira coluna não vazia após a coluna 0; se ainda assim não
    houver, assume a coluna 1.
    """
    header_idx = None
    first_date_col = None
    # padrão de datas ou trimestres. Inclui abreviações de meses (ex.: Mar25),
    # códigos de trimestre (ex.: 2T25), datas no formato dd/mm/yy ou dd/mm/yyyy,
    # bem como datas ISO (yyyy-mm-dd) com ou sem horário.
    date_pattern = re.compile(
        r"(^[A-Za-zÀ-ÿ]{3}/?\d{2}$)|(^[1-4]T\d{2}$)|(^\d{1,2}/\d{2}/\d{2,4}$)|(^\d{4}-\d{2}-\d{2})",
        re.IGNORECASE,
    )
    for required in (2, 1):
        for i, row in df.iterrows():
            date_like = 0
            for cell in row:
                # se for Timestamp ou datetime, conta como data imediatamente
                if isinstance(cell, (pd.Timestamp, datetime)):
                    date_like += 1
                    continue
                if isinstance(cell, str):
                    cell_str = cell.strip()
                    if date_pattern.match(cell_str):
                        date_like += 1
            if date_like >= required:
                header_idx = i
                break
        if header_idx is not None:
            break
    if header_idx is None:
        raise ValueError("Linha de cabeçalho não encontrada na planilha.")
    header = df.iloc[header_idx]
    for j, val in enumerate(header):
        # se o cabeçalho for Timestamp/datetime, assume como primeira coluna de datas
        if isinstance(val, (pd.Timestamp, datetime)) and not pd.isna(val):
            first_date_col = j
            break
        if isinstance(val, str):
            val_str = val.strip()
            if date_pattern.match(val_str):
                first_date_col = j
                break
    if first_date_col is None:
        for j, val in enumerate(header):
            if j > 0 and pd.notna(val):
                first_date_col = j
                break
    if first_date_col is None:
        first_date_col = 1
    return header_idx, first_date_col


def parse_sheet(df: pd.DataFrame) -> List[Tuple[str, str, Any, int]]:
    """Converte uma planilha em formato largo para formato longo.

    Retorna uma lista de tuplas ``(nom_atbt, data_base, vlr_atbt, row_id)``.
    O ``row_id`` identifica a linha original do atributo dentro da aba,
    permitindo diferenciar repetições do mesmo nome de atributo em
    diferentes linhas (que devem receber numeração). Dentro de uma
    mesma linha, múltiplas datas não são consideradas duplicatas.
    """
    header_idx, first_date_col = guess_header_row(df)
    header = df.iloc[header_idx]
    results: List[Tuple[str, str, Any, int]] = []
    attr_counter = 0
    for i in range(header_idx + 1, len(df)):
        row = df.iloc[i]
        attr_parts: List[str] = []
        for j in range(first_date_col):
            cell = row[j]
            if isinstance(cell, str) and cell.strip():
                attr_parts.append(cell.strip())
        if not attr_parts:
            continue
        attr_name = " - ".join(attr_parts)
        # verifique se há ao menos um valor numérico nas colunas de datas
        has_value = False
        for val in row[first_date_col:]:
            if isinstance(val, (int, float)) and not pd.isna(val):
                has_value = True
                break
            # também tentar converter strings numéricas contendo separador de milhar
            if isinstance(val, str):
                try:
                    float(str(val).replace(".", "").replace(",", "."))
                    has_value = True
                    break
                except Exception:
                    continue
        if not has_value:
            continue
        # identifica nova linha de atributo
        row_id = attr_counter
        attr_counter += 1
        for j in range(first_date_col, len(header)):
            date_label = header[j]
            value = row[j]
            if pd.isna(date_label) or pd.isna(value):
                continue
            # converte valor numérico, tratando strings com separadores
            if not isinstance(value, (int, float)):
                try:
                    value = float(str(value).replace(".", "").replace(",", "."))
                except Exception:
                    continue
            results.append((attr_name, str(date_label), value, row_id))
    return results


def process_file(file_path: Path, sheet_targets: List[str], bank_name: str) -> List[Dict[str, Any]]:
    """Processa um arquivo Excel e extrai dados das abas especificadas.

    ``file_path``: caminho do arquivo .xlsx a ser lido.
    ``sheet_targets``: lista de nomes de abas que devem ser processadas.
    ``bank_name``: nome da instituição (usado no campo ``nom_inst``).

    Retorna uma lista de dicionários com os campos:
      - pagina
      - nom_inst
      - nom_atbt
      - data_base
      - vlr_atbt
      - data_divulgacao
      - arquivo_origem
      - data_divulgacao (duplicado)
    """
    records: List[Dict[str, Any]] = []
    data_div = extract_data_divulgacao(file_path.name)
    try:
        xl = pd.ExcelFile(file_path)
    except Exception as e:
        print(f"Erro ao abrir arquivo {file_path}: {e}")
        return records
    sheet_map: Dict[str, str] = {}
    for sh in xl.sheet_names:
        normalized = normalize_name(sh)
        sheet_map[normalized] = sh
    for target in sheet_targets:
        target_norm = normalize_name(target)
        candidate_names: List[str] = []
        for norm_name, real_name in sheet_map.items():
            if target_norm in norm_name:
                candidate_names.append(real_name)
        if not candidate_names:
            continue
        def candidate_key(name: str) -> Tuple[bool, int]:
            norm = normalize_name(name)
            ends = not norm.endswith(target_norm)
            return (ends, len(norm))
        matched_name = sorted(candidate_names, key=candidate_key)[0]
        try:
            df = xl.parse(matched_name, header=None)
        except Exception as e:
            print(f"Erro ao ler aba {matched_name} em {file_path}: {e}")
            continue
        try:
            parsed = parse_sheet(df)
        except Exception as e:
            print(f"Falha ao processar {file_path} - {matched_name}: {e}")
            continue
        attr_counts: Dict[str, int] = {}
        row_id_suffix: Dict[int, str] = {}
        for attr_name, data_base, value, row_id in parsed:
            if row_id not in row_id_suffix:
                count = attr_counts.get(attr_name, 0)
                attr_counts[attr_name] = count + 1
                suffix = f" #{count + 1}" if count > 0 else ""
                row_id_suffix[row_id] = suffix
            suffix = row_id_suffix[row_id]
            nom_atbt_out = f"{attr_name}{suffix}" if suffix else attr_name
            formatted_base = format_data_base(data_base)
            record = {
                "pagina": matched_name.strip(),
                "nom_inst": bank_name,
                "nom_atbt": nom_atbt_out,
                "data_base": formatted_base,
                "vlr_atbt": value,
                "data_divulgacao": data_div,
                "arquivo_origem": file_path.name,
                "data_divulgacao_dup": data_div,
            }
            records.append(record)
    return records


def main() -> None:
    """Função principal que coordena a extração e gravação do CSV para o banco Itaú."""
    input_dir = Path(base_path) / "input" / "itau" / "series_historicas"
    execution_date = datetime.now().strftime("%Y-%m-%d")
    output_dir = Path(base_path) / "refined" / "itau" / "series_historicas" / f"data_ext={execution_date}"
    output_file = output_dir / "itau_series.csv"
    sheet_targets = [
        "NPL_com_TVM",
        "IFRS(17) - Balanço - Ativo",
        "IFRS(17)-Balanço-Passivo e PL ",
        "Sumário_PRO FORMA",
    ]
    if not input_dir.exists():
        print(f"Diretório de entrada não encontrado: {input_dir}")
        return
    excel_files = sorted(p for p in input_dir.iterdir() if p.suffix.lower() == ".xlsx")
    all_records: List[Dict[str, Any]] = []
    for file_path in excel_files:
        records = process_file(file_path, sheet_targets, bank_name="itau")
        all_records.extend(records)
    if not all_records:
        print("Nenhum dado extraído. Verifique se os arquivos e abas estão corretos.")
        return
    df_final = pd.DataFrame(all_records)
    ordered_cols = [
        "pagina",
        "nom_inst",
        "nom_atbt",
        "data_base",
        "vlr_atbt",
        "data_divulgacao",
        "arquivo_origem",
        "data_divulgacao_dup",
    ]
    df_final = df_final[ordered_cols]
    df_final.sort_values(by="data_divulgacao", inplace=True)
    os.makedirs(output_dir, exist_ok=True)
    header = [
        "pagina",
        "nom_inst",
        "nom_atbt",
        "data_base",
        "vlr_atbt",
        "data_divulgacao",
        "arquivo_origem",
        "data_divulgacao",
    ]
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        f.write(",".join(header) + "\n")
        df_final.to_csv(f, index=False, header=False, encoding="utf-8")
    print(f"Arquivo gerado com sucesso: {output_file}")


if __name__ == "__main__":
    main()