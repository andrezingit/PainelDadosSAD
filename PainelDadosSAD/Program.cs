using System.Data;
using System;
using Newtonsoft.Json.Linq;
using Microsoft.Data.SqlClient;
using Dapper;
using System.Xml;
using Newtonsoft.Json;

class Program
{
    static string connectionString = "Data Source=10.1.1.3;Initial Catalog=teste;User ID=sa;Password=@npd.2020;TrustServerCertificate=True";
    static string idProposicao = string.Empty;
    static string _url = string.Empty;
    static int contador = 0;
    static async Task Main(string[] args)
    {
        while (true)
        {
            contador = 0;
            Dictionary<string, string> endpoints = new Dictionary<string, string>
            {
                { "deputados", "https://dadosabertos.camara.leg.br/api/v2/deputados?dataInicio=2018-01-01&ordem=ASC&ordenarPor=nome" },
                { "proposicoes", "https://dadosabertos.camara.leg.br/api/v2/proposicoes?dataInicio=2018-01-01&dataFim=2024-10-16&ordem=ASC&ordenarPor=id" },
                { "orgaos", "https://dadosabertos.camara.leg.br/api/v2/orgaos" },
                { "votacoes", "https://dadosabertos.camara.leg.br/api/v2/votacoes?dataInicio=2019-01-01&ordem=DESC&ordenarPor=dataHoraRegistro" },
                { "partidos", "https://dadosabertos.camara.leg.br/api/v2/partidos" },
                { "legislaturas", "https://dadosabertos.camara.leg.br/api/v2/legislaturas" },
                { "situacao", "https://dadosabertos.camara.leg.br/api/v2/referencias/situacoesProposicao" },
                { "siglaTipo", "https://dadosabertos.camara.leg.br/api/v2/referencias/proposicoes/siglaTipo" }
            };

            Dictionary<int, string> opcoes = new Dictionary<int, string> 
            {
                {1, "deputados"},
                {2, "proposicoes"},
                {3, "orgaos"},
                {4, "votacoes"},
                {5, "partidos"},
                {6, "legislaturas"},
                {7, "situacao"},
                {8, "siglaTipo"},
                {9, "au_prop"},
                {10, "sit_prop"}
            };
            Console.WriteLine("\nDigite a opção ou URL do endpoint da API para buscar autores das proposições ou 'sair' para encerrar:");
            foreach (var opcao in opcoes)
            {
                Console.WriteLine($"{opcao.Key} - {opcao.Value}");
            }
            string url = Console.ReadLine();
            if (url.ToLower() == "sair")
            {
                break;
            }

            int opcaoInt = 0;
            int.TryParse(url, out opcaoInt);
            string tableName = string.Empty;

            if (opcoes.ContainsKey(opcaoInt))
            {
                url = opcoes[opcaoInt];
                tableName = url;
            }
            else
            {
                Console.WriteLine("Digite o nome da tabela onde os dados serão armazenados:");
                tableName = Console.ReadLine();
            }         

            if(url.ToLower() == "au_prop")
            {
                _url = url;
                Console.WriteLine("Processando autores das proposições...");
                CarregaProposicoes();
                continue;
            }else
            if(url.ToLower() == "sit_prop")
            {
                _url = url;
                Console.WriteLine("Processando situação das proposições...");
                CarregaProposicoes();
                continue;
            }
            else
            {
                url = endpoints[url];
                Console.WriteLine("Deseja excluir os dados da tabela antes de importar? (s/n):");
                string truncateResponse = Console.ReadLine();
                bool truncateBeforeInsert = truncateResponse.ToLower() == "s";

                // Executa o processo de busca e armazenamento dos dados
                await ProcessEndpointAsync(url, tableName, truncateBeforeInsert);
            }
        }

        Console.WriteLine("Aplicação encerrada.");
    }

    static void CarregaProposicoes()
    {
        using (IDbConnection connection = new SqlConnection(connectionString))
        {
            //Console.WriteLine("Processando autores das proposições...");

            var proposicoes = connection.Query("SELECT * FROM Proposicoes").ToList();
            proposicoes[1].Id = "2193304";
            foreach (var proposicao in proposicoes)
            {
                idProposicao = proposicao.id;
                string tableName = string.Empty;
                string url = string.Empty;
                
                if (_url == "sit_prop")
                {
                    url = $"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{proposicao.id}";
                    tableName = $"ProposicaoSituacao";
                    ProcessEndpointAsync(url, tableName, false).Wait();
                    //Console.WriteLine("Situações das proposições armazenados com sucesso!");
                }

                if(_url == "au_prop")
                {
                    url = $"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{proposicao.id}/autores";
                    tableName = $"ProposicoesAutores";
                    ProcessEndpointAsync(url, tableName, false).Wait();
                    //Console.WriteLine("Autores das proposições armazenados com sucesso!");
                }
            }
        }
    }

    static async Task ProcessEndpointAsync(string url, string tableName, bool truncateBeforeInsert)
    {
        using (IDbConnection connection = new SqlConnection(connectionString))
        {
            Console.WriteLine($"Processando {tableName}...");

            var allData = await FetchAllPagesAsync(url);

            if (allData.Count > 0)
            {
                JArray dataArray = new JArray(allData);
                //Console.WriteLine($"{tableName} enviando para o banco de dados!");
                StoreData(tableName, dataArray, connection, truncateBeforeInsert);
                //Console.WriteLine($"{tableName} armazenado com sucesso!");
            }
            else
            {
                //Console.WriteLine($"Nenhum dado encontrado para {tableName}.");
            }
        }
    }

    static async Task<JObject> FetchDataAsync(string endpointUrl)
    {
        using (HttpClient client = new HttpClient())
        {
            // Adiciona o cabeçalho Accept para receber JSON
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));

            HttpResponseMessage response = await client.GetAsync(endpointUrl);

            if (response.IsSuccessStatusCode)
            {
                string contentType = response.Content.Headers.ContentType?.MediaType;

                if (contentType == "application/json")
                {
                    string content = await response.Content.ReadAsStringAsync();
                    return JObject.Parse(content);
                }
                else
                {
                    string content = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"Erro: O conteúdo retornado não é JSON. Content-Type: {contentType}");
                    Console.WriteLine($"Conteúdo retornado: {content}");
                    if(contentType == "application/xml")
                    {
                        //converrter xml para json
                        XmlDocument doc = new XmlDocument();
                        doc.LoadXml(content);
                        string jsonText = JsonConvert.SerializeXmlNode(doc["xml"]);
                        return JObject.Parse(jsonText);
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            else
            {
                return null;
            }
        }
    }


    static async Task<List<JObject>> FetchAllPagesAsync(string endpointUrl)
    {
        List<JObject> allData = new List<JObject>();
        string url = endpointUrl;

        while (!string.IsNullOrEmpty(url))
        {
            try
            {
                JObject data = await FetchDataAsync(url);
                if (data == null)
                {
                    Console.Write($"\rErro ao processar {url}: Não foi possível obter os dados.");
                    //break;
                }
                while (data == null || data["dados"].ToString() == string.Empty) { data = await FetchDataAsync(url); }

                if (data["dados"] != null)
                {
                    string dadosString = data["dados"].ToString();
                    JObject dados = new JObject();
                    JToken dadosToken = null;
                    try
                    {
                        dados = JObject.Parse(dadosString);
                    }
                    catch
                    {
                        dadosToken = JToken.Parse(dadosString);
                    }

                    if(dadosToken is JArray)
                    {
                        JArray dadosArray = (JArray)dadosToken;
                        int i = 0;
                        foreach (JObject item in dadosArray)
                        {
                            JObject itemNew = new JObject();
                            if (_url == "au_prop")
                            {
                                item.Add("IdProposicao", idProposicao);
                            }
                                

                            if (_url == "sit_prop")
                            {
                                itemNew.Add("IdProposicao", idProposicao);
                                itemNew.Add("CodSituacao", item["statusProposicao"]["CodSituacao"]);
                                allData.Add(itemNew);
                                break;
                            }

                            allData.Add(item);
                            Console.Write($"\rProcessando item {allData.Count}");
                        }
                    }
                    else
                    {
                        JObject item = dados;
                        JObject itemNew = new JObject();
                        if (_url == "au_prop")
                        {
                            item.Add("IdProposicao", idProposicao);
                        }
                            

                        if (_url == "sit_prop")
                        {
                            itemNew.Add("IdProposicao", idProposicao);
                            var statusProposicao = item["statusProposicao"];
                            itemNew.Add("CodSituacao", item["statusProposicao"]["CodSituacao"]);
                            itemNew.Add("despacho", item["statusProposicao"]["despacho"]);
                            allData.Add(itemNew);
                            break;
                        }
                        contador++;
                        Console.Write($"\rProcessando item {contador}");
                        allData.Add(item);
                    }
                }

                // Encontra o link para a próxima página
                string nextUrl = data["links"]?
                    .FirstOrDefault(l => l["rel"]?.ToString() == "next")?["href"]?.ToString();

                url = nextUrl;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao processar {url}: {ex.Message}");
                break;
            }
        }
        return allData;
    }

    static void StoreData(string tableName, JArray data, IDbConnection connection, bool truncateBeforeInsert)
    {
        if (data.Count == 0) return;

        // Obter as colunas do primeiro objeto
        var columns = ((JObject)data[0]).Properties().Select(p => p.Name).ToList();

        // Verificar se a tabela já existe
        string checkTableExistsSql = $@"
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}')
            BEGIN
                CREATE TABLE [{tableName}] (";
        foreach (var column in columns)
        {
            checkTableExistsSql += $"[{column}] NVARCHAR(MAX),";
        }
        checkTableExistsSql = checkTableExistsSql.TrimEnd(',') + "); END;";

        // Executa a criação da tabela se necessário
        connection.Execute(checkTableExistsSql);

        // Excluir dados da tabela, se necessário
        if (truncateBeforeInsert)
        {
            string truncateSql = $"TRUNCATE TABLE [{tableName}];";
            connection.Execute(truncateSql);
        }

        // Inserir dados
        foreach (var item in data)
        {
            var itemObj = (JObject)item;
            var itemColumns = itemObj.Properties().Select(p => p.Name).ToList();
            var itemValues = itemObj.Properties().Select(p => p.Value.ToString().Replace("'", "''")).ToList();

            string insertSql = $"INSERT INTO [{tableName}] ({string.Join(",", itemColumns.Select(c => $"[{c}]"))}) VALUES ({string.Join(",", itemValues.Select(v => $"'{v}'"))});";
            connection.Execute(insertSql);
        }

        Thread.Sleep(500);
        //Console.WriteLine($"\nRegistros inseridos na tabela {tableName}.");
        Console.Clear();
    }
}
