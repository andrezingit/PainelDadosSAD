using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient; // Ou Microsoft.Data.Sqlite para SQLite
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Newtonsoft.Json.Linq;

namespace CamaraDataFetcher
{
    class Program
    {
        static string connectionString = "Data Source=10.1.1.3;Initial Catalog=teste;User ID=sa;Password=@npd.2020;TrustServerCertificate=True";
        static SemaphoreSlim semaphore = new SemaphoreSlim(5); // Limite de 5 requisições simultâneas

        static async Task Main(string[] args)
        {
            await ProcessEndpointsAsync();
        }

        static async Task ProcessEndpointsAsync()
        {
            var endpoints = new Dictionary<string, string>
            {
                { "deputados", "https://dadosabertos.camara.leg.br/api/v2/deputados?dataInicio=2022-01-01&ordem=ASC&ordenarPor=nome" },
                { "proposicoes", "https://dadosabertos.camara.leg.br/api/v2/proposicoes?dataInicio=2022-01-01&dataFim=2024-10-16&ordem=ASC&ordenarPor=id" },
                { "orgaos", "https://dadosabertos.camara.leg.br/api/v2/orgaos" },
                { "votacoes", "https://dadosabertos.camara.leg.br/api/v2/votacoes?dataInicio=2022-01-01&ordem=DESC&ordenarPor=dataHoraRegistro" },
                { "partidos", "https://dadosabertos.camara.leg.br/api/v2/partidos" },
                { "legislaturas", "https://dadosabertos.camara.leg.br/api/v2/legislaturas" }
            };

            int totalEndpoints = endpoints.Count;
            int processedEndpoints = 0;

            using (IDbConnection connection = new SqlConnection(connectionString))
            {
                foreach (var kvp in endpoints)
                {
                    string deleteData = $"IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{kvp.Key}') BEGIN DELETE FROM {kvp.Key}; END;";
                }

                var tasks = endpoints.Select(async kvp =>
                {
                    await ProcessEndpointAsync(kvp.Key, kvp.Value, connection);
                    int completed = Interlocked.Increment(ref processedEndpoints);
                    Console.WriteLine($"Progresso total: {(completed * 100) / totalEndpoints}%");
                }).ToList();

                await Task.WhenAll(tasks);
            }
        }

        static async Task ProcessEndpointAsync(string tableName, string endpointUrl, IDbConnection connection)
        {
            Console.WriteLine($"Processando {tableName}...");

            var allData = await FetchAllPagesAsync(endpointUrl, tableName);

            if (allData.Count > 0)
            {
                JArray dataArray = new JArray(allData);
                await StoreDataAsync(tableName, dataArray, connection);
                Console.WriteLine($"{tableName} armazenado com sucesso!");
            }
            else
            {
                Console.WriteLine($"Nenhum dado encontrado para {tableName}.");
            }
        }

        static async Task<JObject> FetchDataAsync(string endpointUrl)
        {
            await semaphore.WaitAsync();
            try
            {
                using (HttpClient client = new HttpClient())
                {
                    HttpResponseMessage response = await client.GetAsync(endpointUrl);
                    response.EnsureSuccessStatusCode();
                    string content = await response.Content.ReadAsStringAsync();
                    return JObject.Parse(content);
                }
            }
            finally
            {
                semaphore.Release();
            }
        }

        static async Task<List<JObject>> FetchAllPagesAsync(string endpointUrl, string tableName)
        {
            List<JObject> allData = new List<JObject>();
            string url = endpointUrl;
            int pageCount = 0;

            while (!string.IsNullOrEmpty(url))
            {
                pageCount++;
                try
                {
                    JObject data = await FetchDataAsync(url);

                    if (data["dados"] != null)
                    {
                        allData.AddRange(data["dados"].ToObject<List<JObject>>());
                    }

                    // Exibe o progresso dentro do endpoint
                    Console.WriteLine($"[{tableName}] Páginas processadas: {pageCount}");

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

        static async Task StoreDataAsync(string tableName, JArray data, IDbConnection connection)
        {
            if (data.Count == 0) return;

            // Obter as colunas do primeiro objeto
            var columns = ((JObject)data[0]).Properties().Select(p => p.Name).ToList();

            // Criar tabela dinamicamente
            string createTableSql = GenerateCreateTableSql(tableName, columns);
            await connection.ExecuteAsync(createTableSql);

            // Preparar dados para inserção em lote
            var dataTable = new DataTable();
            foreach (var column in columns)
            {
                dataTable.Columns.Add(column, typeof(string));
            }

            foreach (var item in data)
            {
                var row = dataTable.NewRow();
                foreach (var column in columns)
                {
                    row[column] = item[column]?.ToString();
                }
                dataTable.Rows.Add(row);
            }

            // Inserção em lote usando SqlBulkCopy
            using (var bulkCopy = new SqlBulkCopy((SqlConnection)connection))
            {
                bulkCopy.DestinationTableName = tableName;
                await bulkCopy.WriteToServerAsync(dataTable);
            }
        }

        static string GenerateCreateTableSql(string tableName, List<string> columns)
        {
            string createTableSql = $"IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}') BEGIN CREATE TABLE [{tableName}] (";
            foreach (var column in columns)
            {
                createTableSql += $"[{column}] NVARCHAR(MAX),";
            }
            createTableSql = createTableSql.TrimEnd(',') + "); END;";
            return createTableSql;
        }
    }
}