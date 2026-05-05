import os
import pickle
import socket as _socket
import time
from datetime import datetime
from typing import Any, Callable

import msgpack
import zmq
from zmq import SyncSocket

NOME_SERVIDOR = os.environ.get("SERVIDOR_NOME", _socket.gethostname())
SERVIDOR_HOST = os.environ.get("SERVIDOR_HOST", _socket.gethostname())
MENSAGENS_POR_HEARTBEAT = 10
MENSAGENS_POR_SINCRONIZACAO = 15
TIMEOUT_P2P = 2000  # ms para aguardar resposta de outro servidor

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5558")

ref_socket = context.socket(zmq.REQ)
ref_socket.connect("tcp://referencia:5559")

relogio_logico = 0
mensagens_desde_heartbeat = 0
mensagens_desde_sincronizacao = 0
coordenador = None  # nome do servidor eleito como coordenador
eleicao_pendente = False  # flag para disparar eleição no próximo ciclo do loop


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
    resposta_rank = incrementar_e_enviar_ref("RANK", {"nome": NOME_SERVIDOR, "host": SERVIDOR_HOST})
    rank = resposta_rank.get("rank", -1)
    print(f"[{NOME_SERVIDOR}] Rank obtido: {rank} | host={SERVIDOR_HOST} | relogio={relogio_logico}", flush=True)
except Exception as e:
    print(f"[{NOME_SERVIDOR}] Erro ao obter rank: {e}", flush=True)
    rank = -1

# Porta P2P baseada no rank (5560 + rank)
SERVIDOR_PORT = 5560 + rank

p2p_socket = context.socket(zmq.REP)
p2p_socket.bind(f"tcp://*:{SERVIDOR_PORT}")
print(f"[{NOME_SERVIDOR}] Socket P2P escutando na porta {SERVIDOR_PORT}", flush=True)

poller = zmq.Poller()
poller.register(socket, zmq.POLLIN)
poller.register(p2p_socket, zmq.POLLIN)


def porta_do_rank(r: int) -> int:
    """Retorna a porta P2P de um servidor dado seu rank."""
    return 5560 + r


def enviar_p2p(host: str, porta: int, tarefa: str, argumentos: dict) -> dict | None:
    """Envia mensagem P2P a outro servidor e aguarda resposta com timeout."""
    global relogio_logico
    tmp = context.socket(zmq.REQ)
    tmp.setsockopt(zmq.LINGER, 0)
    tmp.connect(f"tcp://{host}:{porta}")
    relogio_logico += 1
    req = {"tarefa": tarefa, "argumentos": argumentos, "relogio": relogio_logico}
    tmp.send(msgpack.packb(req, use_bin_type=True))

    poll_tmp = zmq.Poller()
    poll_tmp.register(tmp, zmq.POLLIN)
    eventos = dict(poll_tmp.poll(TIMEOUT_P2P))

    resposta = None
    if tmp in eventos:
        raw = tmp.recv()
        resposta = msgpack.unpackb(raw, raw=False)
        relogio_recebido = resposta.get("relogio", 0)
        relogio_logico = max(relogio_logico, relogio_recebido)

    tmp.close()
    return resposta


def sincronizar_relogio_com_coordenador() -> bool:
    """
    Algoritmo de Berkeley: envia REQ ao coordenador pedindo a hora correta.
    O coordenador responde com REP: hora correta.
    Retorna True se sincronizou com sucesso, False caso contrário.
    """
    global relogio_logico, coordenador
    if not coordenador:
        return False

    try:
        resposta_list = incrementar_e_enviar_ref("LIST", {})
        servidores = resposta_list.get("servidores", [])
        entry_coord = next(
            (s for s in servidores if s["nome"] == coordenador), None
        )
        if entry_coord is None:
            print(f"[{NOME_SERVIDOR}] Coordenador '{coordenador}' não encontrado na lista", flush=True)
            return False

        porta_coord = porta_do_rank(entry_coord["rank"])
        host_coord = entry_coord.get("host", coordenador)
        print(
            f"[{NOME_SERVIDOR}] Sincronizando relógio com coordenador '{coordenador}' ({host_coord}:{porta_coord})",
            flush=True,
        )

        resposta = enviar_p2p(host_coord, porta_coord, "RELOGIO", {})
        if resposta and resposta.get("ok"):
            hora_coord = resposta.get("datetime", "")
            print(
                f"[{NOME_SERVIDOR}] Relógio sincronizado com coordenador | hora_coord={hora_coord} | relogio={relogio_logico}",
                flush=True,
            )
            return True
        else:
            print(f"[{NOME_SERVIDOR}] Coordenador não respondeu à sincronização", flush=True)
            return False

    except Exception as e:
        print(f"[{NOME_SERVIDOR}] Erro ao sincronizar relógio: {e}", flush=True)
        return False


def iniciar_eleicao():
    """
    Algoritmo de eleição Bully:
    - Obtém a lista de servidores do referencia
    - Envia REQ: eleicao para servidores com rank maior
    - Se alguém responde OK, aguarda o novo coordenador ser anunciado
    - Se ninguém responde, se auto-elege e publica no tópico 'servers'
    """
    global coordenador, relogio_logico
    print(f"[{NOME_SERVIDOR}] Iniciando eleição (rank={rank})", flush=True)

    try:
        resposta_list = incrementar_e_enviar_ref("LIST", {})
        servidores = resposta_list.get("servidores", [])
    except Exception as e:
        print(f"[{NOME_SERVIDOR}] Erro ao listar servidores para eleição: {e}", flush=True)
        return

    # Servidores com rank maior que o nosso
    candidatos = [s for s in servidores if s["rank"] > rank]

    alguem_respondeu = False
    for candidato in candidatos:
        porta_candidato = porta_do_rank(candidato["rank"])
        host_candidato = candidato.get("host", candidato["nome"])
        print(
            f"[{NOME_SERVIDOR}] Enviando ELEICAO para '{candidato['nome']}' ({host_candidato}:{porta_candidato})",
            flush=True,
        )
        resposta = enviar_p2p(host_candidato, porta_candidato, "ELEICAO", {"nome": NOME_SERVIDOR})
        if resposta and resposta.get("ok"):
            print(
                f"[{NOME_SERVIDOR}] '{candidato['nome']}' respondeu OK à eleição — aguardando coordenador",
                flush=True,
            )
            alguem_respondeu = True
            break

    if not alguem_respondeu:
        # Nenhum servidor com rank maior respondeu — nos auto-elegemos
        coordenador = NOME_SERVIDOR
        print(f"[{NOME_SERVIDOR}] Eleito como coordenador!", flush=True)
        # Publicar no tópico 'servers' que somos o novo coordenador
        pub.send_string("servers", flags=zmq.SNDMORE)
        pub.send_string(coordenador)
        print(f"[{NOME_SERVIDOR}] Publicado novo coordenador no tópico 'servers': {coordenador}", flush=True)


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


def enviar_resposta(sock: SyncSocket, resposta: Any):
    global relogio_logico
    relogio_logico += 1
    resposta["relogio"] = relogio_logico
    print(f"[{NOME_SERVIDOR}] Enviando resposta {resposta} | relogio={relogio_logico}", flush=True)
    sock.send(msgpack.packb(resposta, use_bin_type=True))


def fazer_heartbeat():
    """Envia heartbeat ao referencia."""
    global relogio_logico
    try:
        resposta = incrementar_e_enviar_ref("HEARTBEAT", {"nome": NOME_SERVIDOR, "host": SERVIDOR_HOST})
        print(
            f"[{NOME_SERVIDOR}] Heartbeat OK | relogio={relogio_logico}",
            flush=True,
        )
    except Exception as e:
        print(f"[{NOME_SERVIDOR}] Erro no heartbeat: {e}", flush=True)


def processar_mensagem_cliente(message: bytes):
    """Processa mensagem recebida de um cliente via broker."""
    global relogio_logico, mensagens_desde_heartbeat, mensagens_desde_sincronizacao, coordenador, eleicao_pendente

    try:
        data = msgpack.unpackb(message, raw=False)
        argumentos = data.get("argumentos", {})
        tarefa = data.get("tarefa")
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
            return

        resposta = action[tarefa](**argumentos)
        enviar_resposta(socket, {"ok": True, "mensagem": resposta})

        # Heartbeat a cada MENSAGENS_POR_HEARTBEAT mensagens de clientes
        mensagens_desde_heartbeat += 1
        if mensagens_desde_heartbeat >= MENSAGENS_POR_HEARTBEAT:
            mensagens_desde_heartbeat = 0
            fazer_heartbeat()

        # Sincronização de relógio a cada MENSAGENS_POR_SINCRONIZACAO mensagens
        mensagens_desde_sincronizacao += 1
        if mensagens_desde_sincronizacao >= MENSAGENS_POR_SINCRONIZACAO:
            mensagens_desde_sincronizacao = 0
            if coordenador and coordenador != NOME_SERVIDOR:
                sucesso = sincronizar_relogio_com_coordenador()
                if not sucesso:
                    print(f"[{NOME_SERVIDOR}] Coordenador indisponível, iniciando eleição", flush=True)
                    coordenador = None
                    iniciar_eleicao()
            else:
                # Sem coordenador ou somos o coordenador — inicia/confirma eleição
                iniciar_eleicao()

    except Exception as exc:
        print(exc)
        enviar_resposta(
            socket, {"ok": False, "mensagem": f"Erro ao processar MessagePack: {exc}"}
        )


def processar_mensagem_p2p(message: bytes):
    """Processa mensagem recebida de outro servidor via socket P2P."""
    global relogio_logico, coordenador

    try:
        data = msgpack.unpackb(message, raw=False)
        tarefa = data.get("tarefa")
        argumentos = data.get("argumentos", {})
        relogio_recebido = data.get("relogio", 0)

        relogio_logico = max(relogio_logico, relogio_recebido)
        print(
            f"[{NOME_SERVIDOR}] P2P recebido tarefa={tarefa} relogio_recebido={relogio_recebido} relogio_local={relogio_logico}",
            flush=True,
        )

        if tarefa == "RELOGIO":
            # Berkeley: responder com a hora correta (apenas o coordenador deve receber isso,
            # mas qualquer servidor pode responder caso seja chamado)
            hora_atual = datetime.now().isoformat()
            enviar_resposta(p2p_socket, {"ok": True, "datetime": hora_atual})

        elif tarefa == "ELEICAO":
            # Algoritmo Bully: responder OK indicando que temos rank maior
            nome_remetente = argumentos.get("nome", "")
            print(
                f"[{NOME_SERVIDOR}] Recebeu ELEICAO de '{nome_remetente}', respondendo OK",
                flush=True,
            )
            enviar_resposta(p2p_socket, {"ok": True, "mensagem": "OK"})
            # Sinaliza para o loop principal iniciar eleição no próximo ciclo
            # (não pode chamar iniciar_eleicao() aqui pois usaria ref_socket dentro do handler P2P)
            global eleicao_pendente
            eleicao_pendente = True

        else:
            enviar_resposta(p2p_socket, {"ok": False, "mensagem": "Erro: Comando P2P não reconhecido"})

    except Exception as exc:
        print(f"[{NOME_SERVIDOR}] Erro ao processar mensagem P2P: {exc}", flush=True)
        enviar_resposta(p2p_socket, {"ok": False, "mensagem": f"Erro: {exc}"})


def atualizar_coordenador_por_publicacao(nome_coord: str):
    """Atualiza a variável coordenador quando recebe anúncio via tópico 'servers'."""
    global coordenador
    coordenador = nome_coord
    print(f"[{NOME_SERVIDOR}] Novo coordenador recebido via 'servers': {coordenador}", flush=True)


# Registrar socket de assinatura para receber anúncios de coordenador
sub_coord = context.socket(zmq.SUB)
sub_coord.connect("tcp://proxy:5557")
sub_coord.setsockopt_string(zmq.SUBSCRIBE, "servers")
poller.register(sub_coord, zmq.POLLIN)

while True:
    eventos = dict(poller.poll())

    if socket in eventos:
        message = socket.recv()
        processar_mensagem_cliente(message)

    if p2p_socket in eventos:
        message = p2p_socket.recv()
        processar_mensagem_p2p(message)

    if sub_coord in eventos:
        topico = sub_coord.recv_string()
        nome_coord = sub_coord.recv_string()
        atualizar_coordenador_por_publicacao(nome_coord)

    # Disparar eleição pendente (sinalizada pelo handler P2P) de forma segura
    if eleicao_pendente:
        eleicao_pendente = False
        iniciar_eleicao()
