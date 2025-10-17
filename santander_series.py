"""
Script de consolidação de séries históricas para o banco Santander.

Este script percorre todos os arquivos Excel (.xlsx) na pasta
``input/santander/series_historicas`` a partir de ``base_path`` e extrai
apenas as abas de interesse ('Balanço' e 'DMPL'). O comportamento é
semelhante ao script do Bradesco, realizando correspondência robusta
dos nomes de abas e convertendo as planilhas em formato “longo”.

Os campos de saída são:

  - pagina: nome original da aba
  - nom_inst: "santander"
  - nom_atbt: nome do atributo (com numeração caso haja repetições)
  - data_base: data presente na linha de cabeçalho (ex.: "Jun/25")
  - vlr_atbt: valor numérico extraído da célula
  - data_divulgacao: data derivada do nome do arquivo (ex.: "2025-06-01")
  - arquivo_origem: nome do arquivo fonte
  - data_divulgacao (campo duplicado)

A saída é gravada em ``refined/santander/series_historicas/data_ext=YYYY-MM-DD/santander_series.csv``.

Para utilizar em ambientes distintos, basta alterar a variável ``base_path``.
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
base_path = r"C:\\Users\\BalerionRider\\Documents\\repo\\base_credito"


def normalize_name(name: str) -> str:
    """Normaliza uma string removendo acentos, espaços e hifens."""
    nfkd = unicodedata.normalize("NFKD", name)
    without_accents = "".join(c for c in nfkd if not unicodedata.combining(c))
    cleaned = re.sub(r"[\s\-]+", "", without_accents).lower()
    return cleaned


def extract_data_divulgacao(file_name: str) -> str:
    """Extrai a data de divulgação a partir do padrão trimestre."""
    match = re.search(r"([1-4])T(\d{2})", file_name, flags=re.IGNORECASE)
    if not match:
        return ""
    trimestre = int(match.group(1))
    ano = int(match.group(2))
    ano_full = 2000 + ano
    mes_map = {1: 3, 2: 6, 3: 9, 4: 12}
    mes = mes_map.get(trimestre, 1)
    return f"{ano_full:04d}-{mes:02d}-01"


def format_data_base(label: str) -> str:
    """Formata o rótulo de data_base para YYYY-MM-01.

    Converte 'Mar/25' ou 'Jun25' para 2025-03-01 ou 2025-06-01,
    bem como '1T25' ou '2T25'. Se não conseguir interpretar, retorna
    o rótulo original como string.
    """
    if not isinstance(label, str):
        return str(label)
    text = label.strip()
    m = re.match(r"([1-4])T(\d{2})", text, flags=re.IGNORECASE)
    if m:
        trimestre = int(m.group(1))
        ano = int(m.group(2))
        ano_full = 2000 + ano
        mes_map = {1: 3, 2: 6, 3: 9, 4: 12}
        mes = mes_map.get(trimestre, 1)
        return f"{ano_full:04d}-{mes:02d}-01"
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
        if mes_num is None:
            return text
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

    Procura rótulos de datas ou trimestres. Primeiro busca uma linha
    contendo dois ou mais rótulos; se não existir, aceita uma linha com
    apenas um. O rótulo pode ser tanto mês (ex.: 'Mar/17') quanto
    trimestre (ex.: '2T25'). Se nenhum rótulo for encontrado,
    utiliza a primeira coluna não vazia após a coluna 0 como início.
    """
    header_idx = None
    first_date_col = None
    date_pattern = re.compile(r"(^[A-Za-zÀ-ÿ]{3}/?\d{2}$)|(^[1-4]T\d{2}$)", re.IGNORECASE)
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
        raise ValueError("Linha de cabeçalho não encontrada.")
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
    """Transforma a planilha em formato longo.

    Retorna uma lista de tuplas ``(nom_atbt, data_base, vlr_atbt, row_id)``.
    ``row_id`` identifica cada linha de atributo na aba para permitir
    numeração adequada dos atributos repetidos sem considerar colunas de data.
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
        has_value = False
        for val in row[first_date_col:]:
            if isinstance(val, (int, float)) and not pd.isna(val):
                has_value = True
                break
        if not has_value:
            continue
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
    """Processa um único arquivo .xlsx e retorna registros de dados."""
    records: List[Dict[str, Any]] = []
    data_div = extract_data_divulgacao(file_path.name)
    try:
        xl = pd.ExcelFile(file_path)
    except Exception as e:
        print(f"Erro ao abrir arquivo {file_path}: {e}")
        return records
    sheet_map: Dict[str, str] = {normalize_name(sh): sh for sh in xl.sheet_names}
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
        # Controle de duplicidade por linha de atributo. Só quando o mesmo
        # nome de atributo aparece em linhas diferentes é que numeramos.
        attr_counts: Dict[str, int] = {}
        row_id_suffix: Dict[int, str] = {}
        for attr_name, data_base, value, row_id in parsed:
            # para cada novo row_id calculamos o sufixo
            if row_id not in row_id_suffix:
                count = attr_counts.get(attr_name, 0)
                attr_counts[attr_name] = count + 1
                suffix = f" #{count + 1}" if count > 0 else ""
                row_id_suffix[row_id] = suffix
            suffix = row_id_suffix[row_id]
            nom_atbt_out = f"{attr_name}{suffix}" if suffix else attr_name
            formatted_base = format_data_base(data_base)
            records.append({
                "pagina": matched_name.strip(),
                "nom_inst": bank_name,
                "nom_atbt": nom_atbt_out,
                "data_base": formatted_base,
                "vlr_atbt": value,
                "data_divulgacao": data_div,
                "arquivo_origem": file_path.name,
                "data_divulgacao_dup": data_div,
            })
    return records


def main() -> None:
    """Executa a extração e gravação dos dados consolidados."""
    input_dir = Path(base_path) / "input" / "santander" / "series_historicas"
    execution_date = datetime.now().strftime("%Y-%m-%d")
    output_dir = Path(base_path) / "refined" / "santander" / "series_historicas" / f"data_ext={execution_date}"
    output_file = output_dir / "santander_series.csv"
    sheet_targets = ['Balanço', 'DMPL']
    if not input_dir.exists():
        print(f"Diretório de entrada não encontrado: {input_dir}")
        return
    excel_files = sorted(p for p in input_dir.iterdir() if p.suffix.lower() == ".xlsx")
    all_records: List[Dict[str, Any]] = []
    for file_path in excel_files:
        records = process_file(file_path, sheet_targets, bank_name="santander")
        all_records.extend(records)
    if not all_records:
        print("Nenhum dado extraído para Santander.")
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
    # ordena por data_divulgacao crescente
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