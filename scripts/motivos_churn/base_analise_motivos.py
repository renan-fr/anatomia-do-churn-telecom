import pandas as pd

CONTRATOS_PATH = "data/raw/fato_contratos.csv"
CLIENTES_PATH = "data/raw/dim_clientes.csv"
OUTPUT_PATH = "data/processed/base_contratos_status_churn.csv"

JANELA_DIAS = 90

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

df["status_churn"] = pd.NA
df.loc[df["data_cancelamento"].notna() | (df["status"] == "cancelado"), "status_churn"] = "cancelou"
df.loc[df["data_cancelamento"].isna() & (df["status"] != "cancelado") & df["data_inicio"].notna(), "status_churn"] = "ativo"

faltando_status = df["status_churn"].isna().sum()
if faltando_status > 0:
    raise ValueError(f"Existem {faltando_status} contratos sem status_churn definido (verifique datas/status).")

churn_sem_data = ((df["status_churn"] == "cancelou") & df["data_cancelamento"].isna()).sum()
if churn_sem_data > 0:
    raise ValueError(f"Existem {churn_sem_data} contratos marcados como cancelou sem data_cancelamento.")

ultima_data_observada = pd.concat([df["data_inicio"], df["data_cancelamento"]]).max()
if pd.isna(ultima_data_observada):
    ultima_data_observada = pd.Timestamp.today().normalize()

df["data_referencia"] = df["data_cancelamento"].where(df["status_churn"] == "cancelou", ultima_data_observada)

df["tempo_de_casa_dias"] = (df["data_referencia"] - df["data_inicio"]).dt.days
df["tempo_de_casa_meses"] = (df["tempo_de_casa_dias"] // 30).astype("Int64")

df["inicio_janela_90d"] = df["data_referencia"] - pd.Timedelta(days=JANELA_DIAS)
df["fim_janela_90d"] = df["data_referencia"]

base = df.rename(columns={"estado": "uf"})[
    [
        "contrato_id",
        "customer_id",
        "plano_id",
        "uf",
        "data_inicio",
        "data_cancelamento",
        "status_churn",
        "data_referencia",
        "tempo_de_casa_dias",
        "tempo_de_casa_meses",
        "inicio_janela_90d",
        "fim_janela_90d",
    ]
]

base.to_csv(OUTPUT_PATH, index=False)
print(base.head(20))
