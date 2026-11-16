
import argparse
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3
from botocore.config import Config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream", default="tcc-eventos-transporte")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--simular", action="store_true")
    args = parser.parse_args()

    client = None

    if not args.simular:
        client = boto3.client(
            "kinesis",
            region_name=args.region,
            config=Config(
                connect_timeout=3,
                read_timeout=10,
                retries={"max_attempts": 2, "mode": "standard"}
            )
        )

    class Handler(BaseHTTPRequestHandler):
        def responder(self, status, dados):
            body = json.dumps(dados).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/health":
                self.responder(200, {
                    "status": "ok",
                    "modo": "simulacao" if args.simular else "aws",
                    "stream": args.stream
                })
            else:
                self.responder(404, {"erro": "Rota inexistente"})

        def do_POST(self):
            if self.path != "/evento":
                self.responder(404, {"erro": "Rota inexistente"})
                return

            inicio = time.perf_counter()

            try:
                tamanho = int(self.headers.get("Content-Length", 0))

                if tamanho <= 0 or tamanho > 1048576:
                    self.responder(400, {"erro": "Tamanho invalido"})
                    return

                evento = json.loads(
                    self.rfile.read(tamanho).decode("utf-8")
                )

                obrigatorios = (
                    "id_evento",
                    "veiculo_id",
                    "latitude",
                    "longitude",
                    "velocidade",
                    "temperatura_carga",
                    "status_rota",
                    "data_hora"
                )

                if not isinstance(evento, dict) or any(
                    evento.get(campo) is None for campo in obrigatorios
                ):
                    self.responder(400, {"erro": "Evento incompleto"})
                    return

                payload = json.dumps(
                    evento, separators=(",", ":")
                ).encode("utf-8")

                if args.simular:
                    resultado = {"modo": "simulacao"}
                else:
                    resposta = client.put_record(
                        StreamName=args.stream,
                        PartitionKey=str(evento["veiculo_id"]),
                        Data=payload
                    )

                    resultado = {
                        "modo": "aws",
                        "shard": resposta["ShardId"],
                        "sequence_number": resposta["SequenceNumber"]
                    }

                resultado["id_evento"] = evento["id_evento"]
                resultado["duracao_ms"] = round(
                    (time.perf_counter() - inicio) * 1000, 3
                )

                self.responder(200, resultado)

            except Exception as erro:
                self.responder(502, {
                    "erro": type(erro).__name__
                })

        def log_message(self, formato, *argumentos):
            pass

    servidor = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)

    print(
        f"Produtor iniciado em http://127.0.0.1:{args.port}"
    )
    print(
        "Modo: SIMULACAO" if args.simular
        else f"Modo: AWS - {args.stream}"
    )

    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        servidor.server_close()


if __name__ == "__main__":
    main()
