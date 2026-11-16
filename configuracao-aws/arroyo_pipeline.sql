
CREATE TABLE eventos_transporte (
    id_evento BIGINT,
    veiculo_id BIGINT,
    latitude DOUBLE,
    longitude DOUBLE,
    velocidade DOUBLE,
    temperatura_carga DOUBLE,
    status_rota TEXT,
    data_hora TEXT
) WITH (
    connector = 'kafka',
    type = 'source',
    format = 'json',
    bootstrap_servers = 'kafka:29092',
    topic = 'eventos-transporte',
    'source.offset' = 'latest'
);

CREATE TABLE resultados_arroyo WITH (
    connector = 'kafka',
    type = 'sink',
    format = 'json',
    bootstrap_servers = 'kafka:29092',
    topic = 'resultados-arroyo'
);

INSERT INTO resultados_arroyo
SELECT
    status_rota,
    COUNT(*) AS total_eventos,
    AVG(velocidade) AS velocidade_media,
    AVG(temperatura_carga) AS temperatura_media
FROM eventos_transporte
GROUP BY
    status_rota,
    TUMBLE(INTERVAL '10 seconds');
