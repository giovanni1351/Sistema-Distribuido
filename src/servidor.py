import os
import pickle
import time
from datetime import datetime
from typing import Any, Callable

import msgpack
import zmq
from zmq import SyncSocket

NOME_SERVIDOR = os.environ.get("SERVIDOR_NOME", "servidor")
MENSAGENS_POR_HEARTBEAT = 10

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5558")

ref_socket = context.socket(zmq.REQ)
ref_socket.connect("tcp://referencia:5559")

relogio_logico = 0
mensagens_desde_heartbeat = 0


def incrementar_e_enviar_ref(tarefa: str, argumentos: dict) -> dict:
    """Envia mensagem ao serviço de referência com relógio lógico."""
    global relogio_logico
    relogio_logico += 1
    req = {"tarefa": tarefa, "argumentos": argumentos, "relogio": relogio_logico}
    ref_socket.send(msgpack.packb(req, use_bin_type=True))
    resposta = msgpack.unpackb(ref_socket.recv(), raw=False)
    relogio_recebido = resposta.get("relogio", 0)
    relogio_logico = max(relogio_logico, relogio_recebido)
    return resposta


# Registrar rank ao iniciar
try:
    resposta_rank = incrementar_e_enviar_ref("RANK", {"nome": NOME_SERVIDOR})
    rank = resposta_rank.get("rank", -1)
    print(f"[{NOME_SERVIDOR}] Rank obtido: {rank} | relogio={relogio_logico}", flush=True)
except Exception as e:
    print(f"[{NOME_SERVIDOR}] Erro ao obter rank: {e}", flush=True)
    rank = -1


def ler_dados[T: list | set](entidade: str, container: Callable[[], T] = set) -> T:
    if not os.path.exists(os.path.join("entidades", f"{entidade}.pkl")):
        os.makedirs(os.path.join("entidades"), exist_ok=True)
        salvar_dados(entidade, container())

    with open(os.path.join("entidades", f"{entidade}.pkl"), "rb") as arquivo:
        dados = pickle.load(arquivo)
        return dados


def salvar_dados(entidade: str, dados: Any) -> bool:
    with open(os.path.join("entidades", f"{entidade}.pkl"), "wb") as arquivo:
        pickle.dump(dados, arquivo)
    return True


def criar_usuario(nome_usuario: str, **kargs: Any) -> str:
    usuarios = ler_dados("usuarios")

    if usuarios and nome_usuario in usuarios:
        return f"Usuario {nome_usuario} ja cadastrado"
    usuarios.add(nome_usuario)
    salvar_dados("usuarios", usuarios)
    return f"Usuario '{nome_usuario}' adicionada com sucesso!"


def logar_usuario(nome_usuario: str, **kargs: Any) -> str:
    usuarios = ler_dados("usuarios")
    return (
        "Usuario logado com sucesso"
        if usuarios and nome_usuario in usuarios
        else f"Erro ao realizar o login o usuario {nome_usuario} não existe"
    )


def criar_canal(nome_canal: str, **kargs: Any) -> str:
    canais = ler_dados("canais")
    if canais and nome_canal in canais:
        return f"O Canal {nome_canal} ja foi criado!"

    canais.add(nome_canal)
    salvar_dados("canais", canais)
    return f"Canal '{nome_canal}' adicionada com sucesso!"


def listar_canais(**kargs: Any) -> str:
    canais = ler_dados("canais")
    if not canais:
        return "Nenhum canal criado"
    return ", ".join(canais)


def publicar_no_canal(nome_canal: str, mensagem: str, **kargs: Any) -> str:
    canais = ler_dados("canais")
    if not canais:
        return "Erro: Nenhum canal criado no servidor."

    if nome_canal not in canais:
        return f"Erro: O canal '{nome_canal}' não existe."

    pub.send_string(nome_canal, flags=zmq.SNDMORE)
    pub.send_string(mensagem)
    mensagens: list = ler_dados("mensagens", list)
    mensagens.append(mensagem)
    salvar_dados("mensagens", mensagens)

    return f"Mensagem publicada com sucesso no canal '{nome_canal}'!"


action = {
    "CRIAR_USUARIO": criar_usuario,
    "LOGAR_USUARIO": logar_usuario,
    "CRIAR_CANAL": criar_canal,
    "LISTAR_CANAIS": listar_canais,
    "PUBLICAR_NO_CANAL": publicar_no_canal,
}


def enviar_resposta(socket: SyncSocket, resposta: Any):
    global relogio_logico
    relogio_logico += 1
    resposta["relogio"] = relogio_logico
    print(f"[{NOME_SERVIDOR}] Enviando resposta {resposta} | relogio={relogio_logico}", flush=True)
    socket.send(msgpack.packb(resposta, use_bin_type=True))


def fazer_heartbeat():
    """Envia heartbeat ao referencia e sincroniza o relógio físico."""
    global relogio_logico
    try:
        resposta = incrementar_e_enviar_ref("HEARTBEAT", {"nome": NOME_SERVIDOR})
        datetime_ref = resposta.get("datetime", "")
        print(
            f"[{NOME_SERVIDOR}] Heartbeat OK | datetime_ref={datetime_ref} | relogio={relogio_logico}",
            flush=True,
        )
    except Exception as e:
        print(f"[{NOME_SERVIDOR}] Erro no heartbeat: {e}", flush=True)


while True:
    message = socket.recv()

    try:
        data = msgpack.unpackb(message, raw=False)
        argumentos = data.get("argumentos", {})
        tarefa = data.get("tarefa")
        datetime_recebido = data.get("datetime", "")
        relogio_recebido = data.get("relogio", 0)

        # Sincronizar relógio lógico com o valor recebido do cliente
        relogio_logico = max(relogio_logico, relogio_recebido)
        print(
            f"[{NOME_SERVIDOR}] Recebido tarefa={tarefa} relogio_cliente={relogio_recebido} relogio_local={relogio_logico}",
            flush=True,
        )

        if tarefa not in action.keys():
            enviar_resposta(
                socket, {"ok": False, "mensagem": "Erro: Comando não reconhecido"}
            )
            continue

        resposta = action[tarefa](**argumentos)
        enviar_resposta(socket, {"ok": True, "mensagem": resposta})

        # Heartbeat a cada MENSAGENS_POR_HEARTBEAT mensagens de clientes
        mensagens_desde_heartbeat += 1
        if mensagens_desde_heartbeat >= MENSAGENS_POR_HEARTBEAT:
            mensagens_desde_heartbeat = 0
            fazer_heartbeat()

    except Exception as exc:
        print(exc)
        enviar_resposta(
            socket, {"ok": False, "mensagem": f"Erro ao processar MessagePack: {exc}"}
        )
