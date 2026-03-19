from typing import Literal

import zmq
from time import sleep

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://broker:5555")

def comunicar(comando: Literal["ADICIONAR", "LISTAR", "REMOVER"],tarefa:str| None = None) -> str:
    if tarefa is not None:
        socket.send_string(f"{comando} {tarefa}")
    else:        
        socket.send_string(comando)
    resposta = socket.recv()
    return resposta.decode('utf-8')

i = 0
while True:
    print(f"Enviando comando {i} de adicionar", flush=True)
    print(comunicar("ADICIONAR", f"Tarefa {i}"),flush=True)
    sleep(0.5)
    print(f"Enviando comando {i} de listar", flush=True)
    print(comunicar("LISTAR"),flush=True)
    sleep(0.5)

    print(f"Enviando comando {i} de remover", flush=True)
    print(comunicar("REMOVER", f"Tarefa {i}"),flush=True)
    sleep(0.5)

    i += 1
    sleep(0.5)
