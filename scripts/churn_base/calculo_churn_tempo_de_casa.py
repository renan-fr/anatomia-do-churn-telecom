import pandas as pd

CONTRATOS_PATH = "data/raw/fato_contratos.csv"
OUTPUT_PATH = "data/processed/base/churn_mensal_por_tempo_de_casa.csv"

ROUND_DECIMALS = 4

FAIXAS_TEMPO_DE_CASA = [-1, 30, 90, 180, 10**9]
ROTULOS_FAIXAS = ["0-30 dias", "31-90 dias", "91-180 dias", "180+ dias"]

df = pd.read_csv(CONTRATOS_PATH)

df["data_inicio"] = pd.to_datetime(df["data_inicio"], errors="coerce")
df["data_cancelamento"] = pd.to_datetime(df["data_cancelamento"], errors="coerce")
df["status"] = df["status"].astype(str).str.strip().str.lower()

min_inicio = df["data_inicio"].min()
max_fim = df["data_cancelamento"].max() if df["data_cancelamento"].notna().any() else pd.Timestamp.today()

meses = pd.date_range(
    min_inicio.to_period("M").to_timestamp(),
    max_fim.to_period("M").to_timestamp(),
    freq="MS"
)

cal = pd.DataFrame({"mes_inicio": meses})
cal["mes_fim"] = cal["mes_inicio"] + pd.offsets.MonthEnd(0)

tmp = cal.merge(
    df[["contrato_id", "data_inicio", "data_cancelamento"]],
    how="cross"
)

tmp["ativo_inicio_mes"] = (
    tmp["data_inicio"].notna()
    & (tmp["data_inicio"] <= tmp["mes_inicio"])
    & (tmp["data_cancelamento"].isna() | (tmp["data_cancelamento"] >= tmp["mes_inicio"]))
)

tmp["cancelado_no_mes"] = (
    tmp["data_cancelamento"].notna()
    & (tmp["data_cancelamento"] >= tmp["mes_inicio"])
    & (tmp["data_cancelamento"] <= tmp["mes_fim"])
)

tmp["nova_ativacao_no_mes"] = (
    tmp["data_inicio"].notna()
    & (tmp["data_inicio"] >= tmp["mes_inicio"])
    & (tmp["data_inicio"] <= tmp["mes_fim"])
)

tmp["cancelado_no_mes_e_ativo_inicio"] = tmp["cancelado_no_mes"] & tmp["ativo_inicio_mes"]

tmp["tempo_de_casa_dias"] = (tmp["mes_inicio"] - tmp["data_inicio"]).dt.days

tmp["faixa_tempo_de_casa"] = pd.cut(
    tmp["tempo_de_casa_dias"],
    bins=FAIXAS_TEMPO_DE_CASA,
    labels=ROTULOS_FAIXAS
)

tmp.loc[tmp["nova_ativacao_no_mes"], "faixa_tempo_de_casa"] = "0-30 dias"

tmp["faixa_tempo_de_casa"] = pd.Categorical(
    tmp["faixa_tempo_de_casa"],
    categories=ROTULOS_FAIXAS,
    ordered=True
)

churn_tempo_de_casa_mensal = (
    tmp.groupby(["mes_inicio", "faixa_tempo_de_casa"], observed=True)
       .agg(
           base_ativos_inicio=("ativo_inicio_mes", "sum"),
           cancelamentos=("cancelado_no_mes_e_ativo_inicio", "sum"),
           novas_ativacoes=("nova_ativacao_no_mes", "sum"),
       )
       .reset_index()
)

churn_tempo_de_casa_mensal["net_adds"] = (
    churn_tempo_de_casa_mensal["novas_ativacoes"] - churn_tempo_de_casa_mensal["cancelamentos"]
)

churn_tempo_de_casa_mensal["taxa_churn"] = (
    churn_tempo_de_casa_mensal["cancelamentos"] / churn_tempo_de_casa_mensal["base_ativos_inicio"]
).where(churn_tempo_de_casa_mensal["base_ativos_inicio"] > 0, 0.0).round(ROUND_DECIMALS)

churn_tempo_de_casa_mensal["taxa_crescimento_liquido"] = (
    churn_tempo_de_casa_mensal["net_adds"] / churn_tempo_de_casa_mensal["base_ativos_inicio"]
).where(churn_tempo_de_casa_mensal["base_ativos_inicio"] > 0, 0.0).round(ROUND_DECIMALS)

churn_tempo_de_casa_mensal["mes"] = churn_tempo_de_casa_mensal["mes_inicio"].dt.strftime("%Y-%m")

churn_tempo_de_casa_mensal = churn_tempo_de_casa_mensal[
    [
        "mes",
        "faixa_tempo_de_casa",
        "base_ativos_inicio",
        "cancelamentos",
        "novas_ativacoes",
        "net_adds",
        "taxa_churn",
        "taxa_crescimento_liquido",
    ]
].sort_values(["mes", "faixa_tempo_de_casa"])

churn_tempo_de_casa_mensal.to_csv(OUTPUT_PATH, index=False)
print(churn_tempo_de_casa_mensal.head(20))
