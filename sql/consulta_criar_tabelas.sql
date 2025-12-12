CREATE TABLE "dim_clientes"(
    "customer_id" BIGINT NOT NULL,
    "nome" VARCHAR(255) NOT NULL,
    "idade" INTEGER NOT NULL,
    "genero" VARCHAR(255) CHECK
        ("genero" IN('')) NOT NULL,
        "cidade" VARCHAR(255) NOT NULL,
        "estado" CHAR(2) NOT NULL,
        "tipo_residencia" VARCHAR(255)
    CHECK
        ("tipo_residencia" IN('')) NOT NULL,
        "renda_estimada" FLOAT(53) NOT NULL
);
ALTER TABLE
    "dim_clientes" ADD PRIMARY KEY("customer_id");
CREATE TABLE "dim_planos"(
    "plano_id" BIGINT NOT NULL,
    "nome_plano" VARCHAR(255) NOT NULL,
    "franquia_dados_gb" INTEGER NOT NULL,
    "minutos_inclusos" BIGINT NOT NULL,
    "preco_mensal" FLOAT(53) NOT NULL
);
ALTER TABLE
    "dim_planos" ADD PRIMARY KEY("plano_id");
CREATE TABLE "fato_contratos"(
    "contrato_id" BIGINT NOT NULL,
    "customer_id" BIGINT NOT NULL,
    "plano_id" BIGINT NOT NULL,
    "data_inicio" DATE NOT NULL,
    "data_cancelamento" DATE NOT NULL,
    "fidelidade_meses" INTEGER NOT NULL,
    "status" VARCHAR(255) CHECK
        ("status" IN('')) NOT NULL,
        "motivo_cancelamento" VARCHAR(255) NOT NULL
);
ALTER TABLE
    "fato_contratos" ADD PRIMARY KEY("contrato_id");
CREATE TABLE "fato_pagamentos"(
    "pagamento_id" BIGINT NOT NULL,
    "customer_id" BIGINT NOT NULL,
    "ano_mes" VARCHAR(255) NOT NULL,
    "valor_fatura" FLOAT(53) NOT NULL,
    "valor_pago" FLOAT(53) NOT NULL,
    "dias_atraso" INTEGER NOT NULL,
    "foi_pago" BOOLEAN NOT NULL
);
ALTER TABLE
    "fato_pagamentos" ADD PRIMARY KEY("pagamento_id");
CREATE TABLE "fato_tickets"(
    "ticket_id" BIGINT NOT NULL,
    "customer_id" BIGINT NOT NULL,
    "data_abertura" DATE NOT NULL,
    "data_fechamento" DATE NOT NULL,
    "categoria" VARCHAR(255) NOT NULL,
    "status" VARCHAR(255) CHECK
        ("status" IN('')) NOT NULL,
        "sla_atingido" VARCHAR(255) NOT NULL
);
ALTER TABLE
    "fato_tickets" ADD PRIMARY KEY("ticket_id");
CREATE TABLE "fato_uso_mensal"(
    "uso_mensal_id" BIGINT NOT NULL,
    "customer_id" BIGINT NOT NULL,
    "ano_mes" VARCHAR(255) NOT NULL,
    "dados_consumidos_gb" DECIMAL(8, 2) NOT NULL,
    "minutos_utilizados" BIGINT NOT NULL,
    "sms_enviados" BIGINT NOT NULL,
    "roaming_minutos" BIGINT NOT NULL,
    "congestao_rede" VARCHAR(255) NOT NULL
);
ALTER TABLE
    "fato_uso_mensal" ADD PRIMARY KEY("uso_mensal_id");
ALTER TABLE
    "fato_contratos" ADD CONSTRAINT "fato_contratos_contrato_id_foreign" FOREIGN KEY("contrato_id") REFERENCES "dim_clientes"("customer_id");
ALTER TABLE
    "fato_uso_mensal" ADD CONSTRAINT "fato_uso_mensal_customer_id_foreign" FOREIGN KEY("customer_id") REFERENCES "dim_clientes"("customer_id");
ALTER TABLE
    "fato_contratos" ADD CONSTRAINT "fato_contratos_customer_id_foreign" FOREIGN KEY("customer_id") REFERENCES "dim_planos"("plano_id");
ALTER TABLE
    "fato_pagamentos" ADD CONSTRAINT "fato_pagamentos_pagamento_id_foreign" FOREIGN KEY("pagamento_id") REFERENCES "dim_clientes"("customer_id");
ALTER TABLE
    "fato_tickets" ADD CONSTRAINT "fato_tickets_customer_id_foreign" FOREIGN KEY("customer_id") REFERENCES "dim_clientes"("customer_id");