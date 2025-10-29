"""
Script de consolidação de séries históricas do Banco do Brasil para execução em AWS Glue.

Lê planilhas Excel (.xlsx) no S3, processa as abas de interesse e grava um
CSV consolidado de volta no S3. O processamento é feito com pandas e
boto3; não há uso de Spark neste script.

O código normaliza datas, adiciona uma coluna para o rótulo original
de data e numera atributos repetidos quando necessário. O arquivo
resultante é escrito em formato CSV usando ``put_object``.

Modifique ``base_path`` para apontar para o prefixo S3 desejado
(``s3://bucket/prefix``).
"""

import os
import re
import unicodedata
from datetime import datetime
from io import BytesIO

import boto3  # type: ignore
import pandas as pd  # type: ignore


# caminho base no S3
base_path = "s3://meu-bucket/projeto"


def normalize_name(name):
    nfkd = unicodedata.normalize("NFKD", name)
    without_accents = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[\s\-]+", "", without_accents).lower()


def extract_data_divulgacao(file_name):
    m = re.search(r"([1-4])T(\d{2})", file_name, flags=re.IGNORECASE)
    if not m:
        return ""
    trimestre = int(m.group(1))
    ano = int(m.group(2))
    ano_full = 2000 + ano
    mes_map = {1: 3, 2: 6, 3: 9, 4: 12}
    mes = mes_map.get(trimestre, 1)
    return f"{ano_full:04d}-{mes:02d}-01"


def format_data_base(label):
    import pandas as pd
    from datetime import datetime as _dt
    if isinstance(label, (pd.Timestamp, _dt)):
        dt = pd.to_datetime(label)
        year = dt.year
        month = dt.month
        month_end = ((month - 1) // 3 + 1) * 3
        return f"{year:04d}-{month_end:02d}-01"
    if isinstance(label, str):
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
            if mes_num is not None:
                trimestre_end_map = {
                    1: 3, 2: 3, 3: 3,
                    4: 6, 5: 6, 6: 6,
                    7: 9, 8: 9, 9: 9,
                    10: 12, 11: 12, 12: 12,
                }
                mes_trimestre = trimestre_end_map.get(mes_num, mes_num)
                return f"{ano_full:04d}-{mes_trimestre:02d}-01"
        try:
            dt = pd.to_datetime(text, dayfirst=True, errors='raise')
            year = dt.year
            month = dt.month
            month_end = ((month - 1) // 3 + 1) * 3
            return f"{year:04d}-{month_end:02d}-01"
        except Exception:
            pass
    return str(label)


def guess_header_row(df):
    import pandas as pd
    from datetime import datetime as _dt
    date_pattern = re.compile(
        r"(^[A-Za-zÀ-ÿ]{3}/?\d{2}$)|(^[1-4]T\d{2}$)|(^\d{1,2}/\d{2}/\d{2,4}$)|(^\d{4}-\d{2}-\d{2})",
        re.IGNORECASE,
    )
    header_idx = None
    first_date_col = None
    for required in (2, 1):
        for i, row in df.iterrows():
            date_like = 0
            for cell in row:
                if isinstance(cell, (pd.Timestamp, _dt)):
                    date_like += 1
                    continue
                if isinstance(cell, str):
                    if date_pattern.match(cell.strip()):
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
        if isinstance(val, (pd.Timestamp, _dt)) and not pd.isna(val):
            first_date_col = j
            break
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


def parse_sheet(df):
    header_idx, first_date_col = guess_header_row(df)
    header = df.iloc[header_idx]
    results = []
    attr_counter = 0
    for i in range(header_idx + 1, len(df)):
        row = df.iloc[i]
        attr_parts = []
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
            if isinstance(val, str):
                try:
                    float(str(val).replace(".", "").replace(",", "."))
                    has_value = True
                    break
                except Exception:
                    continue
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
            results.append((attr_name, date_label, value, row_id))
    return results


def list_s3_excels(s3_client, bucket, prefix):
    keys = []
    continuation_token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".xlsx"):
                keys.append(key)
        if resp.get("IsTruncated"):
            continuation_token = resp.get("NextContinuationToken")
        else:
            break
    return keys


def process_s3_file(s3_client, bucket, key, sheet_targets, bank_name):
    records = []
    file_name = os.path.basename(key)
    data_div = extract_data_divulgacao(file_name)
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
    except Exception as e:
        print(f"Erro ao baixar {key}: {e}")
        return records
    try:
        xl = pd.ExcelFile(BytesIO(body))
    except Exception as e:
        print(f"Erro ao abrir {key}: {e}")
        return records
    sheet_map = {normalize_name(sh): sh for sh in xl.sheet_names}
    for target in sheet_targets:
        target_norm = normalize_name(target)
        candidates = [real for norm, real in sheet_map.items() if target_norm in norm]
        if not candidates:
            continue
        def candidate_key(name):
            norm = normalize_name(name)
            ends = not norm.endswith(target_norm)
            return (ends, len(norm))
        matched_name = sorted(candidates, key=candidate_key)[0]
        try:
            df = xl.parse(matched_name, header=None)
        except Exception as e:
            print(f"Erro ao ler aba {matched_name} em {key}: {e}")
            continue
        try:
            parsed = parse_sheet(df)
        except Exception as e:
            print(f"Falha ao processar {key} - {matched_name}: {e}")
            continue
        attr_counts = {}
        row_id_suffix = {}
        for attr_name, date_label, value, row_id in parsed:
            if row_id not in row_id_suffix:
                count = attr_counts.get(attr_name, 0)
                attr_counts[attr_name] = count + 1
                suffix = f" #{count + 1}" if count > 0 else ""
                row_id_suffix[row_id] = suffix
            suffix = row_id_suffix[row_id]
            nom_atbt_out = f"{attr_name}{suffix}" if suffix else attr_name
            formatted_base = format_data_base(date_label)
            records.append({
                "pagina": matched_name.strip(),
                "nom_inst": bank_name,
                "nom_atbt": nom_atbt_out,
                "data_base": formatted_base,
                "data_base_original": str(date_label),
                "vlr_atbt": value,
                "data_divulgacao": data_div,
                "arquivo_origem": file_name,
            })
    return records


def main():
    if not base_path.lower().startswith("s3://"):
        raise ValueError("base_path deve começar com 's3://'")
    _, path_without_scheme = base_path.split("s3://", 1)
    parts = path_without_scheme.split('/', 1)
    bucket = parts[0]
    prefix_base = parts[1] if len(parts) > 1 else ""
    input_prefix = f"{prefix_base}/input/banco_brasil/series_historicas/"
    execution_date = datetime.now().strftime("%Y-%m-%d")
    output_prefix = f"{prefix_base}/refined/banco_brasil/series_historicas/data_ext={execution_date}/banco_brasil_series"
    sheet_targets = [
        'Índices de Atraso',
        'Cobertura de Crédito',
        'Carteira de Crédito',
    ]
    s3_client = boto3.client('s3')
    keys = list_s3_excels(s3_client, bucket, input_prefix)
    all_records = []
    for key in keys:
        all_records.extend(process_s3_file(s3_client, bucket, key, sheet_targets, bank_name="banco_brasil"))
    if not all_records:
        print("Nenhum dado extraído. Verifique se os arquivos e abas estão corretos.")
        return
    # Constrói DataFrame pandas
    df_final = pd.DataFrame(all_records)
    ordered_cols = [
        "pagina",
        "nom_inst",
        "nom_atbt",
        "data_base",
        "data_base_original",
        "vlr_atbt",
        "data_divulgacao",
        "arquivo_origem",
    ]
    df_final = df_final[ordered_cols]
    df_final.sort_values(by="data_divulgacao", inplace=True)
    # Gera CSV em memória
    csv_buffer = df_final.to_csv(index=False, encoding="utf-8")
    # Define chave de saída com extensão .csv
    output_key = f"{output_prefix}.csv"
    # Envia para S3 via boto3
    s3_client.put_object(Bucket=bucket, Key=output_key, Body=csv_buffer.encode("utf-8"))
    print(f"Arquivo gerado com sucesso: s3://{bucket}/{output_key}")


if __name__ == "__main__":
    main()