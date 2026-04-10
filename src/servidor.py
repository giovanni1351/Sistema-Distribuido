import os
import pickle
from typing import Any, Callable

import msgpack
import zmq
from zmq import SyncSocket

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

context_publisher = zmq.Context()
pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5558")


def ler_dados[T: list | set](entidade: str, container: Callable[[], T] = set) -> T:
    # 2. Desserializar (Unpickling) - Carregar do arquivo
    if not os.path.exists(os.path.join("entidades", f"{entidade}.pkl")):
        os.makedirs(os.path.join("entidades"), exist_ok=True)
        salvar_dados(entidade, container())

    with open(os.path.join("entidades", f"{entidade}.pkl"), "rb") as arquivo:
        dados = pickle.load(arquivo)
        # print(f"Lendo os dados da entidade {entidade} do tipo {type(dados)}{dados}")
        return dados


def salvar_dados(entidade: str, dados: Any) -> bool:
    # 1. Serializar (Pickling) - Salvar em um arquivo .pkl
    with open(os.path.join("entidades", f"{entidade}.pkl"), "wb") as arquivo:
        # print(f"Salvando os dados {entidade} {type(dados)} {dados}")
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

    # data = bytes(datetime.now().strftime("%d/%m/%Y, %H:%M:%S"), encoding="utf-8")
    # mensagem = message + data

    # print(f"Publicando no canal '{nome_canal}' - Data: {data}", flush=True)

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
    print(f"Enviado resposta {resposta}")
    socket.send(msgpack.packb(resposta, use_bin_type=True))


while True:
    message = socket.recv()
    # print(f"Mensagem recebida: {message}", flush=True)

    try:
        data = msgpack.unpackb(message, raw=False)
        # print(f"Dados recebidos {data}")
        argumentos = data.get("argumentos", {})
        tarefa = data.get("tarefa")
        datetime_recebido = data.get("datetime", "")
        # print(f"Tarefa ({tarefa}) tipo tarefa ({type(tarefa)})")

        if tarefa not in action.keys():
            enviar_resposta(
                socket, {"ok": False, "mensagem": "Erro: Comando não reconhecido"}
            )
            continue
        resposta = action[tarefa](**argumentos)
        # print(f"Resposta gerada pelo servidor {resposta}")
        enviar_resposta(socket, {"ok": True, "mensagem": resposta})

    except Exception as exc:
        print(exc)
        enviar_resposta(
            socket, {"ok": False, "mensagem": f"Erro ao processar MessagePack: {exc}"}
        )
