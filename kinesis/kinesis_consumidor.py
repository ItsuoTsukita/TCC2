
import argparse
import json
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import boto3

parser = argparse.ArgumentParser()
parser.add_argument("--simular", action="store_true")
parser.add_argument("--stream", default="tcc-eventos-transporte")
parser.add_argument("--region", default="us-east-1")
parser.add_argument("--port", type=int, default=8766)
parser.add_argument("--host", default="127.0.0.1")
args = parser.parse_args()

arquivo_db = Path(__file__).with_name("resultados_kinesis.sqlite")
db = sqlite3.connect(str(arquivo_db), check_same_thread=False, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA journal_mode=WAL")
mutex = threading.Lock()

db.executescript("""
CREATE TABLE IF NOT EXISTS eventos (
    id_evento TEXT PRIMARY KEY,
    veiculo_id TEXT NOT NULL,
    status_rota TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS posicao_atual (
    veiculo_id TEXT PRIMARY KEY,
    latitude REAL,
    longitude REAL,
    velocidade REAL,
    temperatura_carga REAL,
    status_rota TEXT,
    data_hora TEXT
);
CREATE TABLE IF NOT EXISTS resumo_frota (
    status_rota TEXT PRIMARY KEY,
    total_eventos INTEGER NOT NULL,
    soma_velocidade REAL NOT NULL,
    soma_temperatura REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS checkpoints (
    shard_id TEXT PRIMARY KEY,
    sequencia TEXT NOT NULL
);
""")

estado = {"modo": "simulacao" if args.simular else "aws", "erro": None}


def gravar(evento, shard=None, sequencia=None):
    veiculo = str(evento["veiculo_id"])
    identificador = str(evento["id_evento"])
    status = str(evento["status_rota"])
    velocidade = float(evento["velocidade"])
    temperatura = float(evento["temperatura_carga"])

    with mutex:
        with db:
            insercao = db.execute(
                "INSERT OR IGNORE INTO eventos VALUES (?, ?, ?)",
                (identificador, veiculo, status)
            )

            if insercao.rowcount:
                db.execute("""
                    INSERT INTO posicao_atual VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(veiculo_id) DO UPDATE SET
                        latitude=excluded.latitude,
                        longitude=excluded.longitude,
                        velocidade=excluded.velocidade,
                        temperatura_carga=excluded.temperatura_carga,
                        status_rota=excluded.status_rota,
                        data_hora=excluded.data_hora
                    WHERE excluded.data_hora >= posicao_atual.data_hora
                """, (
                    veiculo, float(evento["latitude"]),
                    float(evento["longitude"]), velocidade,
                    temperatura, status, str(evento["data_hora"])
                ))

                db.execute("""
                    INSERT INTO resumo_frota VALUES (?, 1, ?, ?)
                    ON CONFLICT(status_rota) DO UPDATE SET
                        total_eventos=total_eventos+1,
                        soma_velocidade=soma_velocidade+excluded.soma_velocidade,
                        soma_temperatura=soma_temperatura+excluded.soma_temperatura
                """, (status, velocidade, temperatura))

            if shard and sequencia:
                db.execute("""
                    INSERT INTO checkpoints VALUES (?, ?)
                    ON CONFLICT(shard_id) DO UPDATE SET
                        sequencia=excluded.sequencia
                """, (shard, sequencia))

    return insercao.rowcount == 1


class Handler(BaseHTTPRequestHandler):
    def responder(self, codigo, dados):
        corpo = json.dumps(dados, ensure_ascii=False).encode("utf-8")
        self.send_response(codigo)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(corpo)))
        self.end_headers()
        self.wfile.write(corpo)

    def do_GET(self):
        url = urlparse(self.path)

        if url.path == "/health":
            self.responder(200, estado)
            return

        with mutex:
            if url.path == "/veiculo":
                veiculo = parse_qs(url.query).get("id", [""])[0]
                linha = db.execute(
                    "SELECT * FROM posicao_atual WHERE veiculo_id=?",
                    (veiculo,)
                ).fetchone()
                resultado = dict(linha) if linha else None

            elif url.path == "/frota":
                linhas = db.execute("""
                    SELECT status_rota, total_eventos,
                           ROUND(soma_velocidade/total_eventos, 2)
                             AS velocidade_media,
                           ROUND(soma_temperatura/total_eventos, 2)
                             AS temperatura_media
                    FROM resumo_frota
                    ORDER BY status_rota
                """).fetchall()
                resultado = [dict(linha) for linha in linhas]
            else:
                self.responder(404, {"erro": "Rota inexistente"})
                return

        self.responder(200 if resultado is not None else 404,
                      resultado if resultado is not None else
                      {"erro": "Veiculo nao encontrado"})

    def do_POST(self):
        if not args.simular or self.path != "/teste":
            self.responder(403, {"erro": "Operacao indisponivel"})
            return

        try:
            tamanho = int(self.headers.get("Content-Length", "0"))
            if tamanho <= 0 or tamanho > 1048576:
                self.responder(400, {"erro": "Tamanho invalido"})
                return

            evento = json.loads(self.rfile.read(tamanho))
            novo = gravar(evento)
            self.responder(200, {"gravado": novo})
        except (ValueError, TypeError, KeyError) as erro:
            self.responder(400, {"erro": str(erro)})

    def log_message(self, *argumentos):
        pass


def consumir_kinesis():
    cliente = boto3.client("kinesis", region_name=args.region)
    iteradores = {}
    encerrados = set()
    proxima_busca = 0

    while True:
        try:
            if time.monotonic() >= proxima_busca:
                paginas = cliente.get_paginator("list_shards").paginate(
                    StreamName=args.stream
                )

                for pagina in paginas:
                    for shard in pagina["Shards"]:
                        sid = shard["ShardId"]
                        if sid in iteradores or sid in encerrados:
                            continue

                        with mutex:
                            linha = db.execute(
                                "SELECT sequencia FROM checkpoints WHERE shard_id=?",
                                (sid,)
                            ).fetchone()

                        parametros = {
                            "StreamName": args.stream,
                            "ShardId": sid,
                            "ShardIteratorType":
                                "AFTER_SEQUENCE_NUMBER" if linha else "TRIM_HORIZON"
                        }
                        if linha:
                            parametros["StartingSequenceNumber"] = linha[0]

                        iteradores[sid] = cliente.get_shard_iterator(
                            **parametros
                        ).get("ShardIterator")

                proxima_busca = time.monotonic() + 30

            for sid, iterador in list(iteradores.items()):
                if not iterador:
                    encerrados.add(sid)
                    del iteradores[sid]
                    continue

                try:
                    resposta = cliente.get_records(
                        ShardIterator=iterador,
                        Limit=10000
                    )
                except cliente.exceptions.ExpiredIteratorException:
                    del iteradores[sid]
                    proxima_busca = 0
                    continue

                for registro in resposta["Records"]:
                    evento = json.loads(registro["Data"].decode("utf-8"))
                    gravar(evento, sid, registro["SequenceNumber"])

                iteradores[sid] = resposta.get("NextShardIterator")

            estado["erro"] = None
            time.sleep(1)

        except Exception as erro:
            estado["erro"] = type(erro).__name__
            print("Erro ao consumir:", type(erro).__name__, flush=True)
            time.sleep(5)


if not args.simular:
    threading.Thread(target=consumir_kinesis, daemon=True).start()

servidor = ThreadingHTTPServer((args.host, args.port), Handler)
print(f"Consultas: http://{args.host}:{args.port}")
print(f"Modo: {estado['modo']}")

try:
    servidor.serve_forever()
except KeyboardInterrupt:
    pass
finally:
    servidor.server_close()
    db.close()
