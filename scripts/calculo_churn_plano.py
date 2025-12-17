import pandas as pd

df = pd.read_csv("data/raw/fato_contratos.csv")

df["data_inicio"] = pd.to_datetime(df["data_inicio"], errors="coerce")
df["data_cancelamento"] = pd.to_datetime(df["data_cancelamento"], errors="coerce")
df["status"] = df["status"].astype(str).str.strip().str.lower()

df["is_cancelado"] = (df["status"] == "cancelado") | df["data_cancelamento"].notna()

min_inicio = df["data_inicio"].min()
max_fim = (
    df["data_cancelamento"].max()
    if df["data_cancelamento"].notna().any()
    else pd.Timestamp.today()
)

meses = pd.date_range(
    min_inicio.to_period("M").to_timestamp(),
    max_fim.to_period("M").to_timestamp(),
    freq="MS"
)

cal = pd.DataFrame({"mes_inicio": meses})
cal["mes_fim"] = cal["mes_inicio"] + pd.offsets.MonthEnd(0)

tmp = cal.merge(
    df[["contrato_id", "plano_id", "data_inicio", "data_cancelamento"]],
    how="cross"
)

tmp["ativo_inicio_mes"] = (
    tmp["data_inicio"].notna()
    & (tmp["data_inicio"] <= tmp["mes_inicio"])
    & (
        tmp["data_cancelamento"].isna()
        | (tmp["data_cancelamento"] >= tmp["mes_inicio"])
    )
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

churn_plano_mensal = (
    tmp.groupby(["mes_inicio", "plano_id"], as_index=False)
       .agg(
           base_ativos_inicio=("ativo_inicio_mes", "sum"),
           cancelamentos=("cancelado_no_mes", "sum"),
           novas_ativacoes=("nova_ativacao_no_mes", "sum"),
       )
)

churn_plano_mensal["taxa_churn"] = (
    churn_plano_mensal["cancelamentos"]
    / churn_plano_mensal["base_ativos_inicio"]
).where(churn_plano_mensal["base_ativos_inicio"] > 0, 0.0)

churn_plano_mensal["net_adds"] = (
    churn_plano_mensal["novas_ativacoes"]
    - churn_plano_mensal["cancelamentos"]
)

churn_plano_mensal["mes"] = churn_plano_mensal["mes_inicio"].dt.strftime("%Y-%m")

churn_plano_mensal = churn_plano_mensal[
    [
        "mes",
        "plano_id",
        "base_ativos_inicio",
        "cancelamentos",
        "novas_ativacoes",
        "taxa_churn",
        "net_adds",
    ]
]

churn_plano_mensal.to_csv(
    "data/processed/churn_mensal_por_plano.csv",
    index=False
)

print(churn_plano_mensal.head())
