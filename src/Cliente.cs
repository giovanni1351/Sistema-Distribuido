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

        [Key("relogio")]
        public int Relogio { get; set; } = 0;
    }

    [MessagePackObject]
    public class Resposta
    {
        [Key("ok")]
        public bool Ok { get; set; }

        [Key("mensagem")]
        public string Mensagem { get; set; } = string.Empty;

        [Key("relogio")]
        public int Relogio { get; set; } = 0;
    }

    public static RequestSocket client = new RequestSocket();
    public static SubscriberSocket subSocket = new SubscriberSocket();

    public static int relogioLogico = 0;

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
        "Canal_de_Futebol",
        "Canal_de_moda",
        "Canal_de_noticias",
        "Canal_de_jogose_letronicos",
        "Canal_de_Basquete",
        "Canal_de_Minecraft",
        "Canal_de_CounterStrike",
    };
    public static List<string> canaisInscritos = new List<string>();
    public static List<string> mensagens_pub_sub = new List<string>
    {
        "Bom dia pessoal",
        "Tudo bem com vocês?",
        "Como está as coisas ai?",
        "Vocês viram o lançamento da artemis 2?",
        "Estou impressoinado com a tecnologia humana e o nivel que estamos",
        "Eu acho que tudo é ficção, feito por hollywood",
        "Essa artemis ai é uma mentiraiada",
        "Ontem eu dormi mal pra caramba",
        "Qual o nome dos artronautas",
        "Sabia que na lua não é aterrissagem, mas sim alunissagem?",
        "Estou cansado",
        "Estou animado",
    };
    public static Random random = new Random();

    static void Main(string[] args)
    {
        // Inicia o cliente
        Thread.Sleep(5000);
        client.Connect("tcp://broker:5555");
        subSocket.Connect("tcp://proxy:5557");
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
            IniciarEscuta,
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
        // Incrementar relógio lógico antes de enviar
        relogioLogico++;
        var requisicao = new Requisicao
        {
            Argumentos = argumentos,
            Tarefa = tarefa,
            datetime = DateTime.Now.ToString(),
            Relogio = relogioLogico,
        };

        Console.WriteLine($"[relogio={relogioLogico}] Enviando tarefa={tarefa}");

        var dados = MessagePackSerializer.Serialize(requisicao);
        client.SendFrame(dados);

        var respostaBytes = client.ReceiveFrameBytes();
        var resposta = MessagePackSerializer.Deserialize<Resposta>(respostaBytes);

        // Atualizar relógio lógico com o valor recebido
        relogioLogico = Math.Max(relogioLogico, resposta.Relogio);
        Console.WriteLine($"[relogio={relogioLogico}] Resposta recebida tarefa={tarefa}");

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

    static void IniciarEscuta()
    {
        string respostaCanais = SendComunication("LISTAR_CANAIS", new Dictionary<string, string>());
        string[] canaisCriados = respostaCanais.Split(',');
        Console.WriteLine(canaisCriados);

        while (canaisCriados.Length < 5)
        {
            respostaCanais = SendComunication("LISTAR_CANAIS", new Dictionary<string, string>());
            canaisCriados = respostaCanais.Split(',');
            Console.WriteLine("Menos de 5 canais ativos no servidor. Criando um novo...");
            criar_canal();
        }
        int mensagensEnviadas = 10;
        string canalAtualEnvio = "";
        DateTime ultimaMensagem = DateTime.MinValue;

        Console.WriteLine("Escuta iniciada. Entrando no loop infinito...");

        while (true)
        {
            if (
                subSocket.TryReceiveFrameString(
                    TimeSpan.FromMilliseconds(50),
                    out string topicoRecebido
                )
            )
            {
                string mensagemRecebida = subSocket.ReceiveFrameString();
                Console.WriteLine($"[RECEBIDO via {topicoRecebido}]: {mensagemRecebida}");
            }

            if (canaisInscritos.Count < 3)
            {
                string novoCanal = canaisCriados[random.Next(canaisCriados.Length)].Trim();

                // Valida para não se inscrever no mesmo canal duas vezes
                if (!canaisInscritos.Contains(novoCanal))
                {
                    subSocket.Subscribe(novoCanal);
                    canaisInscritos.Add(novoCanal);
                    Console.WriteLine($"O Bot se inscreveu no canal: {novoCanal}");
                }
            }
            if (mensagensEnviadas >= 10)
            {
                canalAtualEnvio = canaisCriados[random.Next(canaisCriados.Length)].Trim();
                mensagensEnviadas = 0;
                Console.WriteLine(
                    $"\nNovo ciclo: Escolhido o canal '{canalAtualEnvio}' para publicações."
                );
            }
            if ((DateTime.Now - ultimaMensagem).TotalSeconds >= 1 && mensagensEnviadas < 10)
            {
                string msg = mensagens_pub_sub[random.Next(mensagens_pub_sub.Count)];
                var sucesso = SendComunication(
                    "PUBLICAR_NO_CANAL",
                    new Dictionary<string, string>
                    {
                        { "nome_canal", canalAtualEnvio },
                        { "mensagem", msg },
                    }
                );

                Console.WriteLine(
                    $"[ENVIADO p/ {canalAtualEnvio}]: {msg} ({mensagensEnviadas + 1}/10) Retorno {sucesso}"
                );

                mensagensEnviadas++;
                ultimaMensagem = DateTime.Now;
            }
        }
    }
}
