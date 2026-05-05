import time
from datetime import datetime
from typing import Any

import msgpack
import zmq
from zmq import SyncSocket

TIMEOUT_HEARTBEAT = 60  # segundos sem heartbeat para remover servidor da lista

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5559")

# {nome: {rank: int, last_heartbeat: float}}
servidores: dict[str, dict] = {}
proximo_rank = 1
logical_clock = 0


def obter_rank(nome: str, host: str = "", **kargs: Any) -> dict:
    global proximo_rank
    if nome not in servidores:
        servidores[nome] = {"rank": proximo_rank, "last_heartbeat": time.time(), "host": host}
        print(f"Servidor '{nome}' registrado com rank {proximo_rank} host '{host}'", flush=True)
        proximo_rank += 1
    else:
        servidores[nome]["last_heartbeat"] = time.time()
        if host:
            servidores[nome]["host"] = host
        print(f"Servidor '{nome}' ja registrado, rank {servidores[nome]['rank']}", flush=True)
    return {"rank": servidores[nome]["rank"]}


def listar_servidores(**kargs: Any) -> dict:
    remover_inativos()
    lista = [{"nome": nome, "rank": info["rank"], "host": info.get("host", "")} for nome, info in servidores.items()]
    print(f"Listando servidores: {lista}", flush=True)
    return {"servidores": lista}


def heartbeat(nome: str, host: str = "", **kargs: Any) -> dict:
    if nome in servidores:
        servidores[nome]["last_heartbeat"] = time.time()
        if host:
            servidores[nome]["host"] = host
        print(f"Heartbeat recebido de '{nome}'", flush=True)
    else:
        # servidor nao cadastrado ainda, registrar
        global proximo_rank
        servidores[nome] = {"rank": proximo_rank, "last_heartbeat": time.time(), "host": host}
        print(f"Servidor '{nome}' registrado via heartbeat com rank {proximo_rank}", flush=True)
        proximo_rank += 1
    return {"mensagem": "OK"}


def remover_inativos():
    agora = time.time()
    inativos = [
        nome
        for nome, info in servidores.items()
        if agora - info["last_heartbeat"] > TIMEOUT_HEARTBEAT
    ]
    for nome in inativos:
        print(f"Servidor '{nome}' removido por inatividade", flush=True)
        del servidores[nome]


action = {
    "RANK": obter_rank,
    "LIST": listar_servidores,
    "HEARTBEAT": heartbeat,
}


def enviar_resposta(socket: SyncSocket, resposta: Any, com_datetime: bool = True):
    global logical_clock
    logical_clock += 1
    resposta["relogio"] = logical_clock
    if com_datetime:
        resposta["datetime"] = datetime.now().isoformat()
    print(f"Enviando resposta {resposta}", flush=True)
    socket.send(msgpack.packb(resposta, use_bin_type=True))


while True:
    message = socket.recv()
    print(f"Mensagem recebida: {message}", flush=True)

    try:
        data = msgpack.unpackb(message, raw=False)
        print(data, flush=True)

        relogio_recebido = data.get("relogio", 0)
        logical_clock = max(logical_clock, relogio_recebido)

        argumentos = data.get("argumentos", {})
        tarefa = data.get("tarefa")
        print(f"Tarefa ({tarefa}) relogio_recebido ({relogio_recebido})", flush=True)

        if tarefa not in action:
            enviar_resposta(socket, {"ok": False, "mensagem": "Erro: Comando nao reconhecido"})
            continue

        resposta = action[tarefa](**argumentos)
        resposta["ok"] = True
        com_datetime = tarefa != "HEARTBEAT"
        enviar_resposta(socket, resposta, com_datetime=com_datetime)

    except Exception as exc:
        enviar_resposta(socket, {"ok": False, "mensagem": f"Erro ao processar: {exc}"})
