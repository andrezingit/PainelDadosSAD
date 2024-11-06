using System.Data;
using System;
using Newtonsoft.Json.Linq;
using Microsoft.Data.SqlClient;
using Dapper;
using System.Xml;
using Newtonsoft.Json;
using System.IO.Pipes;

class Program
{
    //altere a string de conexão conforme sua base de dados
    static string connectionString = "Data Source=andre-pc\\sqlExpress;Initial Catalog=sisUniselva;Integrated Security=True;Connect Timeout=30;Encrypt=False;Trust Server Certificate=False;Application Intent=ReadWrite;Multi Subnet Failover=False";
    static string idProposicao = string.Empty;
    static string _url = string.Empty;
    static int contador = 0;
    static int ano = 1999;
    static int opcaoSel = 0;
    static string idDeputado = string.Empty; 
    static string idLegislatura = string.Empty;
    static async Task Main(string[] args)
    {
        while (true)
        {
            contador = 0;
            Dictionary<string, string> endpoints = new Dictionary<string, string>
            {
                { "deputados", $"https://dadosabertos.camara.leg.br/api/v2/deputados?dataInicio={ano}-02-01&ordem=ASC&ordenarPor=nome&itens=1000" },
                { "partidos", "https://dadosabertos.camara.leg.br/api/v2/partidos" },
                { "legislaturas", "https://dadosabertos.camara.leg.br/api/v2/legislaturas" },
                { "tiposDespesa", "https://dadosabertos.camara.leg.br/api/v2/referencias/deputados/tipoDespesa" }
            };

            Dictionary<int, string> opcoes = new Dictionary<int, string> 
            {
                {1, "deputados"}
                {5, "partidos"},
                {6, "legislaturas"},
                {11, "desp_dep"}
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
                opcaoSel = opcaoInt;
            }
            else
            {
                Console.WriteLine("Digite o nome da tabela onde os dados serão armazenados:");
                tableName = Console.ReadLine();
            }         

            if(url.ToLower() == "desp_dep")
            {
                _url = url;
                Console.WriteLine("Digite a legislatura");
                idLegislatura = Console.ReadLine();
                Console.WriteLine("Processando as despesas...");
                CarregaDeputados();
                continue;
            }
            else
            {
                if (endpoints.ContainsKey(url))
                {
                    url = endpoints[url];
                }
                Console.WriteLine("Deseja excluir os dados da tabela antes de importar? (s/n):");
                string truncateResponse = Console.ReadLine();
                bool truncateBeforeInsert = truncateResponse.ToLower() == "s";

                // Executa o processo de busca e armazenamento dos dados
                if (opcaoSel == 2 || opcaoSel == 4)
                    while(ano <= 2024)
                    {
                        url = url.Replace((ano-1).ToString(), ano.ToString());
                        ano++;
                        if(ano > 2019)
                            truncateBeforeInsert = false;

                        await ProcessEndpointAsync(url, tableName, truncateBeforeInsert);
                    }   
                else
                    await ProcessEndpointAsync(url, tableName, truncateBeforeInsert);

                ano = 2019;
            }
        }

        Console.WriteLine("Aplicação encerrada.");
    }

    static async Task CarregaDeputados()
    {
        using (IDbConnection connection = new SqlConnection(connectionString))
        {
            Console.WriteLine("Processando despesas dos deputados...");
            var deputados = connection.Query($"SELECT id, idLegislatura FROM Deputados WHERE idLegislatura = {idLegislatura} GROUP BY id, idLegislatura").ToList();
            string tableName = "DeputadosDespesas";
            foreach (var deputado in deputados)
            {
                idDeputado = deputado.id;
                if (deputado.idLegislatura != null)
                {
                    deputado.idLegislatura = idLegislatura;
                    string url = $"https://dadosabertos.camara.leg.br/api/v2/deputados/{idDeputado}/despesas?idLegislatura={idLegislatura}&itens=100&ordem=ASC&ordenarPor=ano";
                    await ProcessEndpointAsync(url, tableName, false);
                }
                
            }
        }
    }

    static async Task ProcessEndpointAsync(string url, string tableName, bool truncateBeforeInsert)
    {
        using (IDbConnection connection = new SqlConnection(connectionString))
        {
            Console.WriteLine($"Processando {tableName} {ano}...");
            List<JObject> allData = new List<JObject>();
                    
            allData = await FetchAllPagesAsync(url);
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

                            if(_url == "desp_dep")
                            {
                                item.Add("IdDeputado", idDeputado);
                                item.Add("IdLegislatura", idLegislatura);
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
                            itemNew.Add("CodSituacao", item["statusProposicao"]["codSituacao"]);
                            itemNew.Add("despacho", item["statusProposicao"]["despacho"]);
                            allData.Add(itemNew);
                            break;
                        }


                        if (_url == "desp_dep")
                        {
                            item.Add("IdDeputado", idDeputado);
                            item.Add("IdLegislatura", idLegislatura);
                        }

                        contador++;
                        Console.Write($"\rProcessando item {contador} - {idLegislatura}");
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
