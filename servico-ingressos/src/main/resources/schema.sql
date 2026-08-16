CREATE TABLE IF NOT EXISTS estoque_setor (
    setor VARCHAR(50) PRIMARY KEY,
    quantidade_disponivel INT NOT NULL
);

CREATE TABLE IF NOT EXISTS evento_processado (
    evento_id VARCHAR(36) PRIMARY KEY,
    processado_em TIMESTAMP WITH TIME ZONE NOT NULL
);