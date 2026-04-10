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
        "Canal_de_Futebol",
        "Canal_de_moda",
        "Canal_de_noticias",
        "Canal_de_jogos eletronicos",
        "Canal_de_Basquete",
    };
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

    static void IniciarEscuta()
    {
        using (var subSocket = new SubscriberSocket())
        {
            subSocket.Connect("tcp://localhost:5558");

            List<string> canaisInscritos = new List<string>();

            // 1. Se existirem menos do que 5 canais, criar um novo
            // Fazemos a requisição via REQ para saber quantos existem no backend.
            string respostaCanais = SendComunication(
                "LISTAR_CANAIS",
                new Dictionary<string, string>()
            );

            // ATENÇÃO: Ajuste a forma de separar (Split) de acordo com como seu Python retorna essa lista.
            // Aqui estou assumindo que ele retorna uma string separada por vírgulas.
            int qtdCanaisServidor = string.IsNullOrWhiteSpace(respostaCanais)
                ? 0
                : respostaCanais.Split(',').Length;

            if (qtdCanaisServidor < 5)
            {
                Console.WriteLine(
                    "[SISTEMA] Menos de 5 canais ativos no servidor. Criando um novo..."
                );
                criar_canal();
            }

            // Variáveis de controle para o envio de mensagens no loop
            int mensagensEnviadas = 10; // Começamos em 10 para forçar a escolha do 1º canal logo no início do loop
            string canalAtualEnvio = "";
            DateTime ultimaMensagem = DateTime.MinValue;

            Console.WriteLine("[SISTEMA] Escuta iniciada. Entrando no loop infinito...");

            while (true)
            {
                // --- PARTE 1: LER MENSAGENS (Sem travar a thread) ---
                // Tenta ler por 50 milissegundos. Se não chegar nada, ele passa direto e continua o loop.
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

                // --- PARTE 2: GERENCIAR INSCRIÇÕES ---
                // Se o bot estiver inscrito em menos do que 3 canais, ele escolhe mais um
                if (canaisInscritos.Count < 3)
                {
                    string novoCanal = nomes_canais[random.Next(nomes_canais.Count)];

                    // Valida para não se inscrever no mesmo canal duas vezes
                    if (!canaisInscritos.Contains(novoCanal))
                    {
                        subSocket.Subscribe(novoCanal);
                        canaisInscritos.Add(novoCanal);
                        Console.WriteLine($"[SISTEMA] O Bot se inscreveu no canal: {novoCanal}");
                    }
                }

                // --- PARTE 3: CICLO DE 10 MENSAGENS (Intervalo de 1s) ---
                // Escolhe um novo canal caso já tenha enviado as 10 mensagens
                if (mensagensEnviadas >= 10)
                {
                    canalAtualEnvio = nomes_canais[random.Next(nomes_canais.Count)];
                    mensagensEnviadas = 0;
                    Console.WriteLine(
                        $"\n[SISTEMA] Novo ciclo: Escolhido o canal '{canalAtualEnvio}' para publicações."
                    );
                }

                // Envia 1 mensagem se já se passou 1 segundo desde o último envio
                if ((DateTime.Now - ultimaMensagem).TotalSeconds >= 1 && mensagensEnviadas < 10)
                {
                    string msg = mensagens_pub_sub[random.Next(mensagens_pub_sub.Count)];

                    // Usamos o socket REQ (client) para pedir ao servidor que publique a mensagem na rede.
                    // IMPORTANTE: Ajuste a tarefa "PUBLICAR_MENSAGEM" para o nome que você usou no seu Python.
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
}
