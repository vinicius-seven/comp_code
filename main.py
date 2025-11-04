"""
Processamento de atributos de crédito para o projeto Base Crédito.

Este script é destinado a execução em um ambiente AWS Glue, onde os
arquivos de entrada residem em um bucket S3. A variável ``base_path``
define o caminho base no S3 (no formato ``s3://bucket/prefix``). A
partir desse caminho, o script localiza arquivos de mapeamento,
entradas manuais, histórico e séries históricas dos bancos, e
constrói dois dataframes de saída (``df_folh_ajus`` e ``df_folh_inpu``)
conforme regras de priorização.

Principais etapas:

1. **Leitura do mapeamento de atributos** (``mapeamento_atributos.csv``),
   que especifica como cada atributo deve ser obtido (origem,
   planilha, coluna) ou calculado.

2. **Leitura e transformação dos arquivos de entradas manuais e
   histórico**. Esses arquivos possuem as datas em colunas; são
   “despivotados” para formato longo, resultando em registros por
   atributo e data.

3. **Leitura das séries históricas** dos bancos (Bradesco, Santander,
   Banco do Brasil e Itaú). Entre possíveis execuções anteriores, o
   script mantém apenas a linha mais recente por combinação de
   ``pagina``, ``nom_inst``, ``nom_atbt`` e ``data_base`` (maior
   ``data_divulgacao``).

4. **Construção de dataframes por origem**:

   - **series_temporais**: junta o mapeamento aos valores lidos das
     séries históricas com base em ``nom_inst``, ``nom_planilha`` e
     ``nom_coluna``.
   - **entradas_manuais_atributos**: junta o mapeamento aos valores
     manuais usando as colunas chave (tipo, nom_inst, nom_ind,
     nom_grup, nom_atbt, dat_base_info).
   - **historico_atributos**: similar às entradas manuais.
   - **campos_calculados**: avalia fórmulas matemáticas presentes na
     coluna ``calculo`` do mapeamento, substituindo referências do
     tipo ``[tipo|nom_grup|nom_atbt]`` pelos valores calculados nas
     origens de menor prioridade (séries/histórico). Parênteses e
     operadores ``+``, ``-``, ``*`` e ``/`` são suportados.
   - **ifdata** e **md&a**: estruturas reservadas para futuras
     integrações. Atualmente retornam dataframes vazios.

5. **Unificação e priorização**. Os dataframes de cada origem são
   concatenados e, para cada combinação de chaves
   (tipo, nom_inst, nom_ind, nom_grup, nom_atbt, dat_base_info),
   mantém-se apenas o registro com maior prioridade. A ordem de
   prioridade é: entradas manuais > origens (séries, calculados,
   ifdata, md&a) > histórico.

6. **Separação por tipo**. Os registros são separados em dois
   dataframes finais: ``df_folh_ajus`` (tipo == ``ajust``) e
   ``df_folh_inpu`` (tipo == ``input``).

7. **Gravação das saídas**. Os dataframes finais são salvos como
   CSV em diretórios com a data de extração: ``refined/folh_ajus/data_ext=YYYY-MM-DD/folh_ajus.csv``
   e ``refined/folh_inpu/data_ext=YYYY-MM-DD/folh_inpu.csv``. Se
   executado mais de uma vez no mesmo dia, o arquivo é sobrescrito para
   manter apenas a versão mais recente.

O script evita o uso de tipagem explícita e utiliza boto3 para
interagir com o S3. Comentários são fornecidos em português para
facilitar a manutenção.
"""

import os
import re
import unicodedata
from datetime import datetime
from io import BytesIO, StringIO

import boto3  # type: ignore
import pandas as pd  # type: ignore


# ---------------------------------------------------------------------------
# Variável base_path: defina o caminho base no S3 (ex.: "s3://meu-bucket/projeto").
# Este valor deve ser ajustado conforme o ambiente de execução.
base_path = "s3://meu-bucket/projeto"


def parse_s3_path(s3_path):
    """Divide um caminho S3 em bucket e prefixo."""
    if not s3_path.lower().startswith("s3://"):
        raise ValueError("base_path deve começar com 's3://'")
    path_without_scheme = s3_path.split("s3://", 1)[1]
    parts = path_without_scheme.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix.rstrip("/")


def read_csv_from_s3(s3_client, bucket, key, sep=","):
    """Lê um arquivo CSV do S3 e retorna um DataFrame pandas."""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read().decode("utf-8")
    except Exception as e:
        print(f"Erro ao ler {key}: {e}")
        return pd.DataFrame()
    return pd.read_csv(StringIO(data), sep=sep)


def pivot_attributes(df):
    """Despivotar um DataFrame de atributos (entradas manuais ou histórico).

    Os arquivos de entradas manuais e histórico possuem as datas como
    colunas. Este método converte cada coluna de data em linhas
    contendo 'dat_base_info' e 'valor'.
    """
    if df.empty:
        return df
    id_cols = ["tipo", "nom_inst", "nom_ind", "nom_grup", "nom_atbt"]
    date_cols = [c for c in df.columns if c not in id_cols]
    df_long = df.melt(id_vars=id_cols, value_vars=date_cols,
                      var_name="dat_base_info", value_name="valor")
    return df_long


def load_mapping(s3_client, bucket, prefix_base):
    """Carrega o arquivo de mapeamento de atributos."""
    key = f"{prefix_base}/input/resources/mapeamento_atributos.csv"
    df = read_csv_from_s3(s3_client, bucket, key, sep=";")
    return df


def load_manual(s3_client, bucket, prefix_base):
    """Carrega e pivota o arquivo de entradas manuais."""
    key = f"{prefix_base}/input/resources/entradas_manuais_atributos.csv"
    df = read_csv_from_s3(s3_client, bucket, key, sep=";")
    df_long = pivot_attributes(df)
    # dat_extr_info para entradas manuais: data da execução
    execution_date = datetime.now().strftime("%Y-%m-%d")
    df_long["dat_extr_info"] = execution_date
    return df_long


def load_historico(s3_client, bucket, prefix_base):
    """Carrega e pivota o arquivo de histórico de atributos."""
    key = f"{prefix_base}/input/resources/historico_atributos.csv"
    df = read_csv_from_s3(s3_client, bucket, key, sep=";")
    df_long = pivot_attributes(df)
    # dat_extr_info no histórico será a própria data base
    if not df_long.empty:
        df_long["dat_extr_info"] = df_long["dat_base_info"]
    return df_long


def list_series_files(s3_client, bucket, prefix_base, bank):
    """Lista todos os arquivos CSV de séries históricas de um banco."""
    prefix = f"{prefix_base}/refined/{bank}/series_historicas/"
    keys = []
    continuation_token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".csv"):
                keys.append(key)
        if resp.get("IsTruncated"):
            continuation_token = resp.get("NextContinuationToken")
        else:
            break
    return keys


def load_series_historicas(s3_client, bucket, prefix_base):
    """Lê todos os CSVs de séries históricas dos bancos e mantém o valor mais recente.

    Retorna um DataFrame com colunas: pagina, nom_inst, nom_atbt,
    data_base, data_base_original, vlr_atbt, data_divulgacao.
    """
    bancos = ["bradesco", "santander", "banco_brasil", "itau"]
    frames = []
    for bank in bancos:
        keys = list_series_files(s3_client, bucket, prefix_base, bank)
        for key in keys:
            df = read_csv_from_s3(s3_client, bucket, key)
            if df.empty:
                continue
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["pagina", "nom_inst", "nom_atbt", "data_base",
                                      "data_base_original", "vlr_atbt", "data_divulgacao"])
    df_all = pd.concat(frames, ignore_index=True)
    # mantém apenas o registro com maior data_divulgacao por combinação
    df_all.sort_values(by="data_divulgacao", inplace=True)
    df_latest = df_all.drop_duplicates(subset=["pagina", "nom_inst", "nom_atbt", "data_base"],
                                       keep="last")
    return df_latest


def process_series(mapping_df, series_df):
    """Gera DataFrame de valores de séries históricas conforme o mapeamento.

    Junta mapeamento (origem == 'series_temporais') às séries históricas
    pelo nome da planilha (nom_planilha) e coluna (nom_coluna).
    """
    if mapping_df.empty or series_df.empty:
        return pd.DataFrame()
    # filtra mapeamento para origens de séries
    map_series = mapping_df[mapping_df["origem"].str.contains("series", case=False, na=False)].copy()
    if map_series.empty:
        return pd.DataFrame()
    # realiza join
    merged = map_series.merge(
        series_df,
        left_on=["nom_inst", "nom_planilha", "nom_coluna"],
        right_on=["nom_inst", "pagina", "nom_atbt"],
        how="left",
    )
    # remove registros sem valor
    merged = merged[~merged["vlr_atbt"].isna()]
    if merged.empty:
        return pd.DataFrame()
    # renomeia e seleciona colunas
    out_df = pd.DataFrame({
        "tipo": merged["tipo"],
        "nom_inst": merged["nom_inst"],
        "nom_ind": merged["nom_ind"],
        "nom_grup": merged["nom_grup"],
        "nom_atbt": merged["nom_atbt_x"],  # atributo final
        "dat_base_info": merged["data_base"],
        "valor": merged["vlr_atbt"],
        "dat_extr_info": merged["data_divulgacao"],
    })
    out_df["origem"] = "series"
    out_df["prioridade"] = 2  # prioridade após entradas manuais
    return out_df


def process_manual(mapping_df, manual_df):
    """Junta os valores de entradas manuais ao mapeamento.

    A junção é feita nas colunas tipo, nom_inst, nom_ind, nom_grup,
    nom_atbt e dat_base_info.
    """
    if mapping_df.empty or manual_df.empty:
        return pd.DataFrame()
    merged = mapping_df.merge(
        manual_df,
        on=["tipo", "nom_inst", "nom_ind", "nom_grup", "nom_atbt"],
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame()
    out_df = pd.DataFrame({
        "tipo": merged["tipo"],
        "nom_inst": merged["nom_inst"],
        "nom_ind": merged["nom_ind"],
        "nom_grup": merged["nom_grup"],
        "nom_atbt": merged["nom_atbt"],
        "dat_base_info": merged["dat_base_info"],
        "valor": merged["valor"],
        "dat_extr_info": merged["dat_extr_info"],
    })
    out_df["origem"] = "manual"
    out_df["prioridade"] = 1  # maior prioridade
    return out_df


def process_historico(mapping_df, historico_df):
    """Junta os valores de histórico ao mapeamento.

    A junção é feita nas colunas tipo, nom_inst, nom_ind, nom_grup,
    nom_atbt e dat_base_info.
    """
    if mapping_df.empty or historico_df.empty:
        return pd.DataFrame()
    merged = mapping_df.merge(
        historico_df,
        on=["tipo", "nom_inst", "nom_ind", "nom_grup", "nom_atbt"],
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame()
    out_df = pd.DataFrame({
        "tipo": merged["tipo"],
        "nom_inst": merged["nom_inst"],
        "nom_ind": merged["nom_ind"],
        "nom_grup": merged["nom_grup"],
        "nom_atbt": merged["nom_atbt"],
        "dat_base_info": merged["dat_base_info"],
        "valor": merged["valor"],
        "dat_extr_info": merged["dat_extr_info"],
    })
    out_df["origem"] = "historico"
    out_df["prioridade"] = 3  # menor prioridade
    return out_df


def evaluate_formula(formula, date, tipo, nom_inst, nom_ind, base_df):
    """Avalia uma fórmula de campo calculado para uma data específica.

    A fórmula pode conter expressões envolvendo símbolos de +, -, *, / e
    referências no formato [tipo|nom_grup|nom_atbt]. Para cada token,
    procura-se no ``base_df`` o valor correspondente à combinação de
    tipo, nom_inst, nom_ind, nom_grup, nom_atbt e dat_base_info.
    """
    # copia a fórmula para substituição
    expr = formula
    # encontra todas as referências
    tokens = re.findall(r"\[(.*?)\]", formula)
    for tok in tokens:
        try:
            tok_tipo, tok_grup, tok_atbt = tok.split("|")
        except ValueError:
            continue
        # filtra base_df para encontrar valor
        mask = (
            (base_df["tipo"].astype(str) == tok_tipo)
            & (base_df["nom_inst"] == nom_inst)
            & (base_df["nom_ind"] == nom_ind)
            & (base_df["nom_grup"] == tok_grup)
            & (base_df["nom_atbt"] == tok_atbt)
            & (base_df["dat_base_info"] == date)
        )
        val = base_df.loc[mask, "valor"]
        if len(val) > 0:
            replacement = str(val.iloc[0])
        else:
            replacement = "0"
        # substitui a referência pelo valor
        expr = expr.replace(f"[{tok}]", replacement)
    # avalia a expressão de forma segura
    try:
        # só permite números e operadores + - * / e parênteses
        if not re.match(r"^[0-9+\-*/(). ]+$", expr):
            return None
        result = eval(expr)
    except Exception:
        return None
    return result


def process_calculados(mapping_df, base_df):
    """Processa campos calculados conforme fórmulas no mapeamento.

    Os campos calculados têm origem 'calculado' no mapeamento. Para
    cada combinação de tipo/inst/ind/grup/atbt, avalia a fórmula para
    cada data presente no ``base_df``.
    """
    calculados = mapping_df[mapping_df["origem"].str.contains("calculado", case=False, na=False)].copy()
    if calculados.empty:
        return pd.DataFrame()
    resultados = []
    # datas distintas em base_df
    datas = base_df["dat_base_info"].dropna().unique()
    execution_date = datetime.now().strftime("%Y-%m-%d")
    for _, row in calculados.iterrows():
        tipo = row["tipo"]
        nom_inst = row["nom_inst"]
        nom_ind = row["nom_ind"]
        nom_grup = row["nom_grup"]
        nom_atbt = row["nom_atbt"]
        formula = str(row.get("calculo", "")).strip()
        if not formula:
            continue
        for date in datas:
            val = evaluate_formula(formula, date, tipo, nom_inst, nom_ind, base_df)
            if val is None:
                continue
            resultados.append({
                "tipo": tipo,
                "nom_inst": nom_inst,
                "nom_ind": nom_ind,
                "nom_grup": nom_grup,
                "nom_atbt": nom_atbt,
                "dat_base_info": date,
                "valor": val,
                "dat_extr_info": execution_date,
                "origem": "calculado",
                "prioridade": 2,
            })
    if not resultados:
        return pd.DataFrame()
    return pd.DataFrame(resultados)


def process_ifdata(mapping_df):
    """Placeholder para origem IFData.

    Retorna DataFrame vazio. A leitura via Athena deverá ser
    implementada quando os detalhes estiverem disponíveis.
    """
    return pd.DataFrame()


def process_mda(mapping_df):
    """Placeholder para origem MD&A.

    Retorna DataFrame vazio. A integração será implementada quando a
    fonte estiver definida.
    """
    return pd.DataFrame()


def unify_origins(manual_df, origin_df, calc_df, historico_df):
    """Concatena os dataframes de cada origem e aplica a prioridade.

    Retorna DataFrame unificado com um valor por chave e data.
    """
    frames = []
    if manual_df is not None and not manual_df.empty:
        frames.append(manual_df)
    if origin_df is not None and not origin_df.empty:
        frames.append(origin_df)
    if calc_df is not None and not calc_df.empty:
        frames.append(calc_df)
    if historico_df is not None and not historico_df.empty:
        frames.append(historico_df)
    if not frames:
        return pd.DataFrame()
    df_all = pd.concat(frames, ignore_index=True)
    # ordena por prioridade (1 = manual, 2 = origens/calculados, 3 = historico)
    df_all.sort_values(by=["tipo", "nom_inst", "nom_ind", "nom_grup", "nom_atbt",
                           "dat_base_info", "prioridade"], inplace=True)
    # remove duplicados mantendo a primeira ocorrência (menor prioridade numérica)
    df_dedup = df_all.drop_duplicates(subset=["tipo", "nom_inst", "nom_ind", "nom_grup",
                                             "nom_atbt", "dat_base_info"], keep="first")
    return df_dedup


def save_to_s3(s3_client, bucket, key, df):
    """Grava um DataFrame como CSV no S3."""
    csv_data = df.to_csv(index=False, encoding="utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_data.encode("utf-8"))
    print(f"Salvo em s3://{bucket}/{key}")


def main():
    """Função principal para orquestrar a carga, processamento e escrita das saídas."""
    bucket, prefix_base = parse_s3_path(base_path)
    s3_client = boto3.client("s3")
    # 1. Carrega mapeamento
    mapping_df = load_mapping(s3_client, bucket, prefix_base)
    if mapping_df.empty:
        print("Mapa de atributos vazio ou não encontrado.")
        return
    # 2. Carrega entradas manuais e histórico
    manual_df = load_manual(s3_client, bucket, prefix_base)
    historico_df = load_historico(s3_client, bucket, prefix_base)
    # 3. Carrega séries históricas
    series_df = load_series_historicas(s3_client, bucket, prefix_base)
    # 4. Processa origens
    df_series = process_series(mapping_df, series_df)
    df_manual = process_manual(mapping_df, manual_df)
    df_historico = process_historico(mapping_df, historico_df)
    # Base para cálculos: une séries e histórico (origem de menor prioridade). Manual não entra nos cálculos.
    base_calc_df = pd.concat([df_series, df_historico], ignore_index=True)
    # Aqui precisa remover duplicados mantendo apenas o melhor
    
    df_calculado = process_calculados(mapping_df, base_calc_df)
    # origens futuras (ifdata e md&a)
    df_ifdata = process_ifdata(mapping_df)
    df_mda = process_mda(mapping_df)
    # concatena origens series, ifdata, mda, calculado (prioridade 2)
    df_orig = pd.concat([df_series, df_ifdata, df_mda], ignore_index=True)
    # 5. Unifica e aplica prioridade
    df_unificado = unify_origins(df_manual, df_orig, df_calculado, df_historico)
    if df_unificado.empty:
        print("Nenhum dado combinado para exportar.")
        return
    # 6. Separa por tipo
    df_folh_ajus = df_unificado[df_unificado["tipo"].str.lower() == "ajust"].copy()
    df_folh_inpu = df_unificado[df_unificado["tipo"].str.lower() == "input"].copy()
    # Seleciona e ordena colunas finais
    final_cols = ["nom_inst", "nom_ind", "nom_grup", "nom_atbt", "valor",
                  "dat_base_info", "dat_extr_info"]
    df_folh_ajus = df_folh_ajus[final_cols]
    df_folh_inpu = df_folh_inpu[final_cols]
    # 7. Salva no S3
    # diretórios de saída com data de extração; sobrescreve se já existir
    execution_date = datetime.now().strftime("%Y-%m-%d")
    ajus_key = f"{prefix_base}/refined/folh_ajus/data_ext={execution_date}/folh_ajus.csv"
    inpu_key = f"{prefix_base}/refined/folh_inpu/data_ext={execution_date}/folh_inpu.csv"
    # Mudar para nossa função padrão de write e para parquet
    save_to_s3(s3_client, bucket, ajus_key, df_folh_ajus)
    save_to_s3(s3_client, bucket, inpu_key, df_folh_inpu)
    print("Processamento concluído com sucesso.")


if __name__ == "__main__":
    main()