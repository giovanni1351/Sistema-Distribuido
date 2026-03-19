import zmq
import msgpack

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")
TAREFAS: list[str] = []


def adicionar_tarefa(tarefa: str) -> str:
    TAREFAS.append(tarefa)
    return f"Tarefa '{tarefa}' adicionada com sucesso!"

def listar_tarefas() -> str:
    return " ".join(TAREFAS)

def remover_tarefa(tarefa: str) -> str:
    if tarefa in TAREFAS:
        TAREFAS.remove(tarefa)
        return f"Tarefa '{tarefa}' removida com sucesso!"
    else:
        return f"Tarefa '{tarefa}' não encontrada."

action ={
    "ADICIONAR": adicionar_tarefa,
    "LISTAR": listar_tarefas,
    "REMOVER": remover_tarefa
}


while True:
    message = socket.recv()
    print(f"Mensagem recebida: {message}", flush=True)

    try:
        data = msgpack.unpackb(message, raw=False)
        command = str(data.get("comando", "")).upper()
        tarefa = data.get("tarefa")

        if command in action:
            if command in ("ADICIONAR", "REMOVER"):
                if tarefa is None or str(tarefa).strip() == "":
                    resposta = {"ok": False, "mensagem": "Erro: tarefa obrigatória"}
                else:
                    result = action[command](str(tarefa))
                    resposta = {"ok": True, "mensagem": result}
            else:
                result = action[command]()
                resposta = {"ok": True, "mensagem": result}
        else:
            resposta = {"ok": False, "mensagem": "Erro: Comando não reconhecido"}
    except Exception as exc:
        resposta = {"ok": False, "mensagem": f"Erro ao processar MessagePack: {exc}"}

    socket.send(msgpack.packb(resposta, use_bin_type=True))