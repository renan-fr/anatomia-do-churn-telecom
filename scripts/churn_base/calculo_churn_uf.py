import pandas as pd

CONTRATOS_PATH = "data/raw/fato_contratos.csv"
CLIENTES_PATH = "data/raw/dim_clientes.csv"
OUTPUT_PATH = "data/processed/base/churn_mensal_por_uf.csv"

ROUND_DECIMALS = 4

contratos = pd.read_csv(CONTRATOS_PATH)
clientes = pd.read_csv(CLIENTES_PATH)

contratos["data_inicio"] = pd.to_datetime(contratos["data_inicio"], errors="coerce")
contratos["data_cancelamento"] = pd.to_datetime(contratos["data_cancelamento"], errors="coerce")
contratos["status"] = contratos["status"].astype(str).str.strip().str.lower()

clientes["estado"] = (
    clientes["estado"]
    .fillna("Não informado")
    .astype(str)
    .str.strip()
    .str.upper()
)

df = contratos.merge(
    clientes[["customer_id", "estado"]],
    on="customer_id",
    how="left"
)

df["estado"] = df["estado"].fillna("Não informado")

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
    df[["contrato_id", "estado", "data_inicio", "data_cancelamento"]],
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

churn_uf_mensal = (
    tmp.groupby(["mes_inicio", "estado"], as_index=False)
       .agg(
           base_ativos_inicio=("ativo_inicio_mes", "sum"),
           cancelamentos=("cancelado_no_mes", "sum"),
           novas_ativacoes=("nova_ativacao_no_mes", "sum"),
       )
)

churn_uf_mensal["net_adds"] = churn_uf_mensal["novas_ativacoes"] - churn_uf_mensal["cancelamentos"]

churn_uf_mensal["taxa_churn"] = (
    churn_uf_mensal["cancelamentos"] / churn_uf_mensal["base_ativos_inicio"]
).where(churn_uf_mensal["base_ativos_inicio"] > 0, 0.0).round(ROUND_DECIMALS)

churn_uf_mensal["taxa_crescimento_liquido"] = (
    churn_uf_mensal["net_adds"] / churn_uf_mensal["base_ativos_inicio"]
).where(churn_uf_mensal["base_ativos_inicio"] > 0, 0.0).round(ROUND_DECIMALS)

churn_uf_mensal["mes"] = churn_uf_mensal["mes_inicio"].dt.strftime("%Y-%m")
churn_uf_mensal = churn_uf_mensal.drop(columns=["mes_inicio"]).rename(columns={"estado": "uf"})

churn_uf_mensal = churn_uf_mensal[
    [
        "mes",
        "uf",
        "base_ativos_inicio",
        "cancelamentos",
        "novas_ativacoes",
        "net_adds",
        "taxa_churn",
        "taxa_crescimento_liquido",
    ]
]

churn_uf_mensal.to_csv(OUTPUT_PATH, index=False)
print(churn_uf_mensal.head())
