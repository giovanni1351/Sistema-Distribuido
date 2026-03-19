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

    public static List<string> nomes = new List<string>
    {
        "Giovanni",
        "Lucas",
        "Roberto",
        "Matheus",
        "Tiago",
        "Henrique",
    };
    public static List<string> nomes_canais = new List<string>
    {
        "Canal de Futebol",
        "Canal de moda",
        "Canal de noticias",
        "Canal de jogos eletronicos",
    };
    public static Random random = new Random();

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
        List<Action> metodos = new List<Action>
        {
            criar_canal,
            listar_canais,
            criar_usuario,
            logar_usuario,
        };
        try
        {
            int i = 0;
            while (true)
            {
                metodos[random.Next(metodos.Count)]();
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

    static void logar_usuario()
    {
        var nome = nomes[random.Next(nomes.Count)];
        Console.WriteLine($"Enviando logar usuario usuario {nome}");
        Console.WriteLine(
            SendComunication(
                "LOGAR_USUARIO",
                new Dictionary<string, string> { { $"nome_usuario", nome } }
            )
        );
        Thread.Sleep(2000);
    }

    static void criar_usuario()
    {
        var nome = nomes[random.Next(nomes.Count)];
        Console.WriteLine($"Enviando criar usuario {nome}");
        Console.WriteLine(
            SendComunication(
                "CRIAR_USUARIO",
                new Dictionary<string, string> { { $"nome_usuario", nome } }
            )
        );
        Thread.Sleep(2000);
    }

    static void listar_canais()
    {
        Console.WriteLine("Enviando listar canais");
        Console.WriteLine(SendComunication("LISTAR_CANAIS", new Dictionary<string, string>()));
        Thread.Sleep(2000);
    }

    static void criar_canal()
    {
        Console.WriteLine("Enviando criar canal");
        Console.WriteLine(
            SendComunication(
                "CRIAR_CANAL",
                new Dictionary<string, string>
                {
                    { $"nome_canal", nomes_canais[random.Next(nomes_canais.Count)] },
                }
            )
        );
        Thread.Sleep(2000);
    }
}
