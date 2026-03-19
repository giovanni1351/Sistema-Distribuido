from typing import Any

import msgpack
import zmq
from zmq import SyncSocket

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")
CANAIS: set[str] = set()

USUARIOS: set[str] = set()


def criar_usuario(nome_usuario: str, **kargs: Any) -> str:
    if nome_usuario in USUARIOS:
        return f"Usuario {nome_usuario} ja cadastrado"
    USUARIOS.add(nome_usuario)
    return f"Usuario '{nome_usuario}' adicionada com sucesso!"


def logar_usuario(nome_usuario: str, **kargs: Any) -> str:
    return (
        "Usuario logado com sucesso"
        if nome_usuario in USUARIOS
        else "Erro ao realizar o login"
    )


def criar_canal(nome_canal: str, **kargs: Any) -> str:
    CANAIS.add(nome_canal)
    return f"Canal '{nome_canal}' adicionada com sucesso!"


def listar_canais(**kargs: Any) -> str:
    return " ".join(CANAIS)


action = {
    "CRIAR_USUARIO": criar_usuario,
    "LOGAR_USUARIO": logar_usuario,
    "CRIAR_CANAL": criar_canal,
    "LISTAR_CANAIS": listar_canais,
}


def enviar_resposta(socket: SyncSocket, resposta: Any):
    print(f"Enviado resposta {resposta}")
    socket.send(msgpack.packb(resposta, use_bin_type=True))


while True:
    message = socket.recv()
    print(f"Mensagem recebida: {message}", flush=True)

    try:
        data = msgpack.unpackb(message, raw=False)
        print(data)
        argumentos = data.get("argumentos", {})
        tarefa = data.get("tarefa")
        datetime = data.get("datetime", "")
        print(f"Tarefa ({tarefa}) tipo tarefa ({type(tarefa)})")

        if tarefa not in action.keys():
            enviar_resposta(
                socket, {"ok": False, "mensagem": "Erro: Comando não reconhecido"}
            )
            continue
        resposta = action[tarefa](**argumentos)
        print(f"Resposta gerada pelo servidor {resposta}")
        enviar_resposta(socket, {"ok": False, "mensagem": resposta})

    except Exception as exc:
        enviar_resposta(
            socket, {"ok": False, "mensagem": f"Erro ao processar MessagePack: {exc}"}
        )
