CREATE TABLE IF NOT EXISTS estoque_setor (
    setor VARCHAR(50) PRIMARY KEY,
    quantidade_disponivel INT NOT NULL
);

CREATE TABLE IF NOT EXISTS evento_processado (
    evento_id VARCHAR(36) PRIMARY KEY,
    processado_em TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Agregador da aula 03: total de ingressos por (evento, setor) em janelas de
-- 1 minuto alinhadas por relogio, usando o event time (reservadoEm) do fato.
CREATE TABLE IF NOT EXISTS agregacao_reserva_por_setor_janela (
    evento VARCHAR(100) NOT NULL,
    setor VARCHAR(50) NOT NULL,
    janela_inicio TIMESTAMP WITH TIME ZONE NOT NULL,
    total_ingressos INT NOT NULL,
    atualizado_em TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (evento, setor, janela_inicio)
);

MERGE INTO estoque_setor (setor, quantidade_disponivel) KEY (setor) VALUES
    ('PISTA', 100),
    ('CAMAROTE', 20),
    ('ARQUIBANCADA', 50);