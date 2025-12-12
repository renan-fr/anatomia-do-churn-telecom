import pandas as pd

df = pd.read_csv("data/raw/fato_contratos.csv")

df["data_inicio"] = pd.to_datetime(df["data_inicio"], errors="coerce")
df["data_cancelamento"] = pd.to_datetime(df["data_cancelamento"], errors="coerce")
df["status"] = df["status"].astype(str).str.strip().str.lower()

df["is_cancelado"] = (df["status"] == "cancelado") | df["data_cancelamento"].notna()

min_inicio = df["data_inicio"].min()
max_fim = (df["data_cancelamento"].max()
           if df["data_cancelamento"].notna().any()
           else pd.Timestamp.today())

meses = pd.date_range(
    min_inicio.to_period("M").to_timestamp(),
    max_fim.to_period("M").to_timestamp(),
    freq="MS"
)

cal = pd.DataFrame({"mes_inicio": meses})
cal["mes_fim"] = cal["mes_inicio"] + pd.offsets.MonthEnd(0)

tmp = cal.merge(df[["contrato_id", "data_inicio", "data_cancelamento"]], how="cross")

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

tmp["novas_ativacoes_no_mes"] = (
    tmp["data_inicio"].notna()
    & (tmp["data_inicio"] >= tmp["mes_inicio"])
    & (tmp["data_inicio"] <= tmp["mes_fim"])
)

churn_mensal = (
    tmp.groupby("mes_inicio", as_index=False)
       .agg(
           base_ativos_inicio=("ativo_inicio_mes", "sum"),
           cancelados_no_mes=("cancelado_no_mes", "sum"),
           novas_ativacoes_no_mes=("novas_ativacoes_no_mes", "sum"),
       )
)

churn_mensal["taxa_churn"] = (
    churn_mensal["cancelados_no_mes"] / churn_mensal["base_ativos_inicio"]
).where(churn_mensal["base_ativos_inicio"] > 0, 0.0)

churn_mensal["saldo_liquido"] = (
    churn_mensal["novas_ativacoes_no_mes"] - churn_mensal["cancelados_no_mes"]
)

churn_mensal["base_ativos_fim_estim"] = (
    churn_mensal["base_ativos_inicio"] + churn_mensal["saldo_liquido"]
)

churn_mensal.to_csv("data/processed/churn_mensal.csv", index=False)