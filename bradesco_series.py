"""
Script de consolidação de séries históricas para o banco Bradesco.

Este script percorre todos os arquivos Excel (.xlsx) encontrados na pasta
``input/bradesco/series_historicas`` a partir de um caminho base definido
na variável ``base_path``. Em cada arquivo são lidas apenas as abas
de interesse ('13- Carteira Expandida ', '12- Carteira Segreg Modalidade ',
'Carteira Crédito - Indicadores' e 'Carteira Expandida - Reclas.'), porém
de forma robusta: ele tolera espaços extras, diferenças simples de
acentuação e maiúsculas/minúsculas, bastando que o nome da aba contenha
o trecho esperado. Para cada aba processada, os dados de atributos
(``nom_atbt``), datas (``data_base``) e valores (``vlr_atbt``) são
extraídos e convertidos para formato “longo” (uma linha por atributo
e por data). A coluna ``pagina`` mantém o nome original da aba, e a
coluna ``arquivo_origem`` armazena o nome do arquivo analisado.

O campo ``data_divulgacao`` é derivado do nome do arquivo a partir de
um padrão como ``1T25`` ou ``4T24`` e convertido para a data do
primeiro dia do último mês do trimestre correspondente. Caso o padrão
não seja encontrado, ``data_divulgacao`` fica vazio. O valor de
``data_divulgacao`` aparece duas vezes no CSV, conforme solicitado.

A saída é escrita em ``refined/bradesco/series_historicas/data_ext=YYYY-MM-DD/bradesco_series.csv``,
onde ``YYYY-MM-DD`` corresponde à data da execução do script. Se
executado mais de uma vez no mesmo dia, o arquivo é sobrescrito.

Para utilizar o script em outro ambiente (por exemplo, AWS Glue), basta
ajustar a variável ``base_path`` para o caminho adequado (por exemplo,
``s3://meu-bucket/projeto``). Nenhuma outra alteração de código é
necessária.
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
    # mês de fim de trimestre: 1->3, 2->6, 3->9, 4->12
    mes_map = {1: 3, 2: 6, 3: 9, 4: 12}
    mes = mes_map.get(trimestre, 1)
    return f"{ano_full:04d}-{mes:02d}-01"


def format_data_base(label: str) -> str:
    """Formata o rótulo de data para o padrão YYYY-MM-01.

    Aceita formatos como 'Mar/25', 'Jun25', '1T25' e retorna o primeiro
    dia do mês correspondente ao final do trimestre: 03 (março), 06 (junho),
    09 (setembro) ou 12 (dezembro). Se não conseguir interpretar,
    retorna o rótulo original.
    """
    if not isinstance(label, str):
        return str(label)
    text = label.strip()
    # trimestre, ex: 2T25
    m = re.match(r"([1-4])T(\d{2})", text, flags=re.IGNORECASE)
    if m:
        trimestre = int(m.group(1))
        ano = int(m.group(2))
        ano_full = 2000 + ano
        mes_map = {1: 3, 2: 6, 3: 9, 4: 12}
        mes = mes_map.get(trimestre, 1)
        return f"{ano_full:04d}-{mes:02d}-01"
    # mês, ex: Mar/25 ou Mar25
    m2 = re.match(r"([A-Za-zÀ-ÿ]{3})/?(\d{2})", text, flags=re.IGNORECASE)
    if m2:
        mes_str = m2.group(1).lower()
        ano = int(m2.group(2))
        ano_full = 2000 + ano
        # mapeia abreviações em português e variantes
        mes_map_dict = {
            'jan': 1, 'fev': 2, 'feb': 2, 'mar': 3,
            'abr': 4, 'apr': 4, 'mai': 5, 'may': 5,
            'jun': 6, 'jul': 7, 'ago': 8,
            'set': 9, 'sep': 9,
            'out': 10, 'oct': 10,
            'nov': 11, 'dez': 12, 'dec': 12,
        }
        mes_num = mes_map_dict.get(mes_str[:3], None)
        if mes_num is None:
            return text
        # converte mês para trimestre final (mar -> 3, abr/mai/jun -> 6, etc.)
        trimestre_end_map = {
            1: 3, 2: 3, 3: 3,
            4: 6, 5: 6, 6: 6,
            7: 9, 8: 9, 9: 9,
            10: 12, 11: 12, 12: 12,
        }
        mes_trimestre = trimestre_end_map.get(mes_num, mes_num)
        return f"{ano_full:04d}-{mes_trimestre:02d}-01"
    return text


def guess_header_row(df: pd.DataFrame) -> Tuple[int, int]:
    """Identifica a linha de cabeçalho e a coluna inicial de datas.

    Esta função procura uma linha contendo rótulos de datas ou de
    trimestres (por exemplo ``Mar/17``, ``Jun25`` ou ``2T25``). Tenta
    primeiro encontrar uma linha com pelo menos dois rótulos; se não
    encontrar, aceita linhas com um rótulo. A coluna inicial é
    determinada pelo primeiro rótulo encontrado. Se nada for
    identificado, usa a primeira coluna não vazia após a coluna 0, e
    caso nenhuma seja encontrada, assume a coluna 1.
    """
    header_idx = None
    first_date_col = None
    # padrão que reconhece rótulos de meses (três letras) ou de trimestres
    date_pattern = re.compile(r"(^[A-Za-zÀ-ÿ]{3}/?\d{2}$)|(^[1-4]T\d{2}$)", re.IGNORECASE)
    # tenta primeiro encontrar linha com dois ou mais rótulos
    for required in (2, 1):
        for i, row in df.iterrows():
            date_like = 0
            for cell in row:
                if isinstance(cell, str) and date_pattern.match(cell.strip()):
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
        if isinstance(val, str) and date_pattern.match(val.strip()):
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
    attr_counter = 0  # identifica cada linha de atributo
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
        has_value = False
        for val in row[first_date_col:]:
            if isinstance(val, (int, float)) and not pd.isna(val):
                has_value = True
                break
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
    # extrai data_divulgacao do nome do arquivo
    data_div = extract_data_divulgacao(file_path.name)
    try:
        xl = pd.ExcelFile(file_path)
    except Exception as e:
        print(f"Erro ao abrir arquivo {file_path}: {e}")
        return records
    # cria dicionário de sheet_name normalizado para nome real
    sheet_map: Dict[str, str] = {}
    for sh in xl.sheet_names:
        normalized = normalize_name(sh)
        sheet_map[normalized] = sh
    # para cada aba alvo, encontrar nas abas do arquivo
    for target in sheet_targets:
        target_norm = normalize_name(target)
        # encontra todas as abas cujo nome normalizado contém o alvo
        candidate_names: List[str] = []
        for norm_name, real_name in sheet_map.items():
            if target_norm in norm_name:
                candidate_names.append(real_name)
        if not candidate_names:
            continue
        # se houver mais de uma, escolher aquela cujo nome normalizado termina com o alvo,
        # ou a de nome normalizado mais curto, para evitar escolher versões com números/prefixos
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
        # controle de repetição por linha de atributo
        attr_counts: Dict[str, int] = {}
        row_id_suffix: Dict[int, str] = {}
        for attr_name, data_base, value, row_id in parsed:
            # apenas quando um novo row_id aparece incrementa-se o contador
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
    """Função principal que coordena a extração e gravação do CSV."""
    # define pastas de entrada e saída
    input_dir = Path(base_path) / "input" / "bradesco" / "series_historicas"
    execution_date = datetime.now().strftime("%Y-%m-%d")
    output_dir = Path(base_path) / "refined" / "bradesco" / "series_historicas" / f"data_ext={execution_date}"
    output_file = output_dir / "bradesco_series.csv"
    sheet_targets = [
        '13- Carteira Expandida ',
        '12- Carteira Segreg Modalidade ',
        'Carteira Crédito - Indicadores',
        'Carteira Expandida - Reclas.',
    ]
    # coleta todos arquivos .xlsx
    if not input_dir.exists():
        print(f"Diretório de entrada não encontrado: {input_dir}")
        return
    excel_files = sorted(p for p in input_dir.iterdir() if p.suffix.lower() == ".xlsx")
    all_records: List[Dict[str, Any]] = []
    for file_path in excel_files:
        records = process_file(file_path, sheet_targets, bank_name="bradesco")
        all_records.extend(records)
    if not all_records:
        print("Nenhum dado extraído. Verifique se os arquivos e abas estão corretos.")
        return
    # cria DataFrame final com colunas na ordem especificada
    df_final = pd.DataFrame(all_records)
    # reorganiza colunas na ordem especificada. Manteremos data_divulgacao duplicado
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
    # ordena por data_divulgacao crescente
    df_final.sort_values(by="data_divulgacao", inplace=True)
    # o pandas não permite colunas duplicadas com o mesmo nome; para gerar
    # o CSV com 'data_divulgacao' repetido, escreveremos o cabeçalho
    # manualmente e salvaremos o conteúdo sem cabeçalho.
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
    # abre o arquivo e escreve cabeçalho e dados
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        f.write(",".join(header) + "\n")
        df_final.to_csv(f, index=False, header=False, encoding="utf-8")
    print(f"Arquivo gerado com sucesso: {output_file}")


if __name__ == "__main__":
    main()