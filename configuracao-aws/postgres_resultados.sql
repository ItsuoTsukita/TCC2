CREATE TABLE IF NOT EXISTS posicao_atual (
    veiculo_id BIGINT PRIMARY KEY,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    velocidade DOUBLE PRECISION,
    temperatura_carga DOUBLE PRECISION,
    status_rota TEXT,
    data_hora TIMESTAMP
);
CREATE TABLE IF NOT EXISTS resumo_frota (
    status_rota TEXT PRIMARY KEY,
    total_eventos BIGINT,
    velocidade_media DOUBLE PRECISION,
    temperatura_media DOUBLE PRECISION
);
