using System.Data;
using System;
using Newtonsoft.Json.Linq;
using Microsoft.Data.SqlClient;
using Dapper;

class Program
{
    static string connectionString = "Data Source=10.1.1.3;Initial Catalog=teste;User ID=sa;Password=@npd.2020;TrustServerCertificate=True";

    static async Task Main(string[] args)
    {
        while (true)
        {
            Console.WriteLine("Digite a URL do endpoint da API ou 'sair' para encerrar:");
            string url = Console.ReadLine();
            if (url.ToLower() == "sair")
            {
                break;
            }

            Console.WriteLine("Digite o nome da tabela onde os dados serão armazenados:");
            string tableName = Console.ReadLine();

            Console.WriteLine("Deseja excluir os dados da tabela antes de importar? (s/n):");
            string truncateResponse = Console.ReadLine();
            bool truncateBeforeInsert = truncateResponse.ToLower() == "s";

            // Executa o processo de busca e armazenamento dos dados
            await ProcessEndpointAsync(url, tableName, truncateBeforeInsert);
        }

        Console.WriteLine("Aplicação encerrada.");
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
                StoreData(tableName, dataArray, connection, truncateBeforeInsert);
                Console.WriteLine($"{tableName} armazenado com sucesso!");
            }
            else
            {
                Console.WriteLine($"Nenhum dado encontrado para {tableName}.");
            }
        }
    }

    static async Task<JObject> FetchDataAsync(string endpointUrl)
    {
        using (HttpClient client = new HttpClient())
        {
            HttpResponseMessage response = await client.GetAsync(endpointUrl);
            response.EnsureSuccessStatusCode();
            string content = await response.Content.ReadAsStringAsync();
            return JObject.Parse(content);
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

                if (data["dados"] != null)
                {
                    allData.AddRange(data["dados"].ToObject<List<JObject>>());
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
    }
}
