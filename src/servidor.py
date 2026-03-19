import os
import pickle
from typing import Any

import msgpack
import zmq
from zmq import SyncSocket

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")


def ler_dados(entidade: str) -> set[str]:
    # 2. Desserializar (Unpickling) - Carregar do arquivo
    if not os.path.exists(os.path.join("entidades", f"{entidade}.pkl")):
        os.makedirs(os.path.join("entidades"), exist_ok=True)
        salvar_dados(entidade, set())

    with open(os.path.join("entidades", f"{entidade}.pkl"), "rb") as arquivo:
        dados = pickle.load(arquivo)
        print(f"Lendo os dados da entidade {entidade} do tipo {type(dados)}{dados}")
        return dados


def salvar_dados(entidade: str, dados: Any) -> bool:
    # 1. Serializar (Pickling) - Salvar em um arquivo .pkl
    with open(os.path.join("entidades", f"{entidade}.pkl"), "wb") as arquivo:
        print(f"Salvando os dados {entidade} {type(dados)} {dados}")
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
        else "Erro ao realizar o login"
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
