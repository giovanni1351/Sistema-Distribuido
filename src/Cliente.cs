using System;
using System.Threading;
using MessagePack;
using NetMQ;
using NetMQ.Sockets;

class Program
{
    [MessagePackObject]
    public class Requisicao
    {
        [Key("comando")]
        public string Comando { get; set; } = string.Empty;

        [Key("tarefa")]
        public string? Tarefa { get; set; }
    }

    [MessagePackObject]
    public class Resposta
    {
        [Key("ok")]
        public bool Ok { get; set; }

        [Key("mensagem")]
        public string Mensagem { get; set; } = string.Empty;
    }

    static void Main(string[] args)
    {
        // Inicia o cliente
        RodarCliente();
    }

    static void RodarCliente()
    {
        // O RequestSocket envia requisições
        Thread.Sleep(9000);
        var client = new RequestSocket();
        client.Connect("tcp://broker:5555");

        try
        {
            int i = 0;
            while (true)
            {
                Console.WriteLine(Comunicar("ADICIONAR", $"Tarefa {i}", client));
                Thread.Sleep(500);

                Console.WriteLine(Comunicar("LISTAR", null, client));
                Thread.Sleep(500);

                Console.WriteLine(Comunicar("REMOVER", $"Tarefa {i}", client));
                Thread.Sleep(500);

                i++;
            }
        }
        finally
        {
            // Sem using: libera recursos explicitamente.
            client.Dispose();
            NetMQConfig.Cleanup();
        }
    }

    static string Comunicar(string comando, string? tarefa, RequestSocket cliente)
    {
        var requisicao = new Requisicao { Comando = comando, Tarefa = tarefa };

        var dados = MessagePackSerializer.Serialize(requisicao);
        cliente.SendFrame(dados);

        var respostaBytes = cliente.ReceiveFrameBytes();
        var resposta = MessagePackSerializer.Deserialize<Resposta>(respostaBytes);
        return resposta.Mensagem;
    }
}
