CREATE TABLE IF NOT EXISTS eventos_transporte (
    id_evento BIGINT,
    veiculo_id BIGINT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    velocidade DOUBLE PRECISION,
    temperatura_carga DOUBLE PRECISION,
    status_rota VARCHAR,
    data_hora TIMESTAMP
);