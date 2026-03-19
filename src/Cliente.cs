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
        [Key("argumentos")]
        public Dictionary<string, string> Argumentos { get; set; } =
            new Dictionary<string, string> { { "nome_usuario", "giovanni" } };

        [Key("tarefa")]
        public string? Tarefa { get; set; }

        [Key("datetime")]
        public string datetime = DateTime.Now.ToString();
    }

    [MessagePackObject]
    public class Resposta
    {
        [Key("ok")]
        public bool Ok { get; set; }

        [Key("mensagem")]
        public string Mensagem { get; set; } = string.Empty;
    }

    public static RequestSocket client = new RequestSocket();

    static void Main(string[] args)
    {
        // Inicia o cliente
        Thread.Sleep(5000);
        client.Connect("tcp://broker:5555");
        RodarCliente();
    }

    static void RodarCliente()
    {
        // O RequestSocket envia requisições

        try
        {
            int i = 0;
            while (true)
            {
                Console.WriteLine(
                    SendComunication(
                        "CRIAR_USUARIO",
                        new Dictionary<string, string> { { $"nome_usuario", "Giovanni" } }
                    )
                );
                Thread.Sleep(2000);

                Console.WriteLine(
                    SendComunication(
                        "LOGAR_USUARIO",
                        new Dictionary<string, string> { { $"nome_usuario", "Giovanni" } }
                    )
                );
                Thread.Sleep(2000);

                Console.WriteLine(
                    SendComunication(
                        "CRIAR_CANAL",
                        new Dictionary<string, string> { { $"nome_canal", "Chat1" } }
                    )
                );
                Thread.Sleep(2000);

                Console.WriteLine(
                    SendComunication("LISTAR_CANAIS", new Dictionary<string, string>())
                );
                Thread.Sleep(2000);

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

    static string SendComunication(string tarefa, Dictionary<string, string> argumentos)
    {
        var requisicao = new Requisicao { Argumentos = argumentos, Tarefa = tarefa };

        var dados = MessagePackSerializer.Serialize(requisicao);
        client.SendFrame(dados);

        var respostaBytes = client.ReceiveFrameBytes();
        var resposta = MessagePackSerializer.Deserialize<Resposta>(respostaBytes);
        return resposta.Mensagem;
    }
}
