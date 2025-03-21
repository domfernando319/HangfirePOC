using System;
using System.Data;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading.Tasks;
using Hangfire;
using Hangfire.SqlServer;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Hangfire.AspNetCore;
using MySqlConnector;
using System.Data.Common;
using System.Data.SqlClient;

namespace HangfireService {
    public interface ITenantService {
        Task EnsureDatabaseAndTableExist(string connectionString);
    }

    public class TenantService : ITenantService {
        public async Task EnsureDatabaseAndTableExist(string connectionString) {
            try{
                using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync();

                string createTableQuery = @"
                    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Message')
                    BEGIN
                        CREATE TABLE [dbo].[Message] (
                            [Id] INT IDENTITY(1,1) PRIMARY KEY,
                            [Message] NVARCHAR(MAX) NOT NULL,
                            [CreatedAt] DATETIME DEFAULT GETDATE()
                        )
                    END";
                using var cmd = new SqlCommand(createTableQuery, connection);
                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine($"SUCCESS: Table 'Message' ensured in database: {connection.Database}");
            } catch (Exception e) {
                Console.WriteLine($"ERROR: {e.Message}");
            }
        }
    }

    public interface ITenantJobService {
        Task LogMessage(string connectionString);
    }
    public class TenantJobService : ITenantJobService {
        private readonly Dictionary<string, IBackgroundJobClient> _jobClients;

        // Modified constructor to accept tenant-specific job clients
        public TenantJobService(Dictionary<string, IBackgroundJobClient> jobClients) {
            _jobClients = jobClients;
        }
        public async Task LogMessage(string connectionString) {
            try {
                using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync();

                string insertQuery = "INSERT INTO [dbo].[Message] ([Message]) VALUES (@message)";
                using var cmd = new SqlCommand(insertQuery, conn);
                cmd.Parameters.AddWithValue("@message", $"Logged at {DateTime.Now}");
                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine($"[{DateTime.Now}] SUCCESS: Message inserted into {conn.Database}");

                // ***Schedule the next execution dynamically using tenant specific storage
                var interval = Program.Databases[connectionString];
                var dbName = new SqlConnectionStringBuilder(connectionString).InitialCatalog;
                var queueName = $"queue-{dbName.ToLower()}";
                var jobId = _jobClients[connectionString].Schedule<ITenantJobService>(
                    queueName,
                    service => service.LogMessage(connectionString), 
                    TimeSpan.FromSeconds(interval)
                );
                Console.WriteLine($"[{DateTime.Now}] Scheduled job {jobId} for {dbName} on queue {queueName}");
            } catch (Exception e) {
                Console.WriteLine($"ERROR: {e.Message}");
            }
        }
    }

    static class Program {
        public static readonly Dictionary<string, int> Databases = new()
        {
            { "Server=DUSFSpectre\\SQLEXPRESS;Database=HangfireDB1;User Id=sa;Password=0319;", 10 }, // Replace with your SQL Server password
            { "Server=DUSFSpectre\\SQLEXPRESS;Database=HangfireDB2;User Id=sa;Password=0319;", 20 },
            { "Server=DUSFSpectre\\SQLEXPRESS;Database=HangfireDB3;User Id=sa;Password=0319;", 30 },
            { "Server=DUSFSpectre\\SQLEXPRESS;Database=HangfireDB4;User Id=sa;Password=0319;", 40 },
            { "Server=DUSFSpectre\\SQLEXPRESS;Database=HangfireDB5;User Id=sa;Password=0319;", 50 },
            { "Server=DUSFSpectre\\SQLEXPRESS;Database=HangfireDB6;User Id=sa;Password=0319;", 60 },
            { "Server=DUSFSpectre\\SQLEXPRESS;Database=HangfireDB7;User Id=sa;Password=0319;", 60 },
            { "Server=DUSFSpectre\\SQLEXPRESS;Database=HangfireDB8;User Id=sa;Password=0319;", 60 },
            { "Server=DUSFSpectre\\SQLEXPRESS;Database=HangfireDB9;User Id=sa;Password=0319;", 60 },
            { "Server=DUSFSpectre\\SQLEXPRESS;Database=HangfireDB10;User Id=sa;Password=0319;", 60 }
        };

        public static readonly Dictionary<string, SqlServerStorage> TenantStorages = new();

        // Helper method to extract database name from connection string
        private static string GetDatabaseNameFromConnectionString(string connectionString)
        {
            var builder = new SqlConnectionStringBuilder(connectionString);
            return builder.InitialCatalog;
        }

        static async Task Main() {
            using var host = Host.CreateDefaultBuilder()
                .ConfigureServices((context, services) => {
                    services.AddSingleton<ITenantService, TenantService>();
                    services.AddTransient<ITenantJobService, TenantJobService>();

                    var jobClients = new Dictionary<string, IBackgroundJobClient>();

                    // Configure Hangfire for each tenant
                    foreach (var (connectionString, _) in Databases) {
                        var storageOptions = new SqlServerStorageOptions {
                            QueuePollInterval = TimeSpan.FromSeconds(1),
                            CommandBatchMaxTimeout = TimeSpan.FromMinutes(5),
                            SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5),
                            PrepareSchemaIfNecessary = true
                        };

                        var storage = new SqlServerStorage(connectionString, storageOptions);
                        TenantStorages[connectionString] = storage;
                        jobClients[connectionString] = new BackgroundJobClient(storage);

                        string dbName = GetDatabaseNameFromConnectionString(connectionString);
                        string queueName = $"queue-{dbName.ToLower()}";

                        // Add Hangfire configuration for this tenant
                        services.AddHangfire((provider, config) => config
                            .SetDataCompatibilityLevel(CompatibilityLevel.Version_170)
                            .UseSimpleAssemblyNameTypeSerializer()
                            .UseRecommendedSerializerSettings()
                            .UseStorage(storage));

                        // Add a dedicated Hangfire server for this tenant
                        services.AddHangfireServer((provider, options) => {
                            options.ServerName = $"Server-{dbName}";
                            options.Queues = new[] {queueName}; // Match the queue used in Enqueue / Schedule
                            options.WorkerCount = 2;
                            options.SchedulePollingInterval = TimeSpan.FromSeconds(1); // Faster job pickup
                        }, storage);
                    }

                    // Register TenantJobService with job clients
                    services.AddTransient<ITenantJobService>(provider => 
                        new TenantJobService(jobClients));
                })
                .Build();

            using var scope = host.Services.CreateScope();
            var tenantService = scope.ServiceProvider.GetRequiredService<ITenantService>();
            var tenantJobService = scope.ServiceProvider.GetRequiredService<ITenantJobService>();

            // Initialize databases and schedule initial jobs
            foreach (var (connString, _) in Databases) {
                await tenantService.EnsureDatabaseAndTableExist(connString);
                var dbName = GetDatabaseNameFromConnectionString(connString);
                var queueName = $"queue-{dbName.ToLower()}";
                var jobClient = new BackgroundJobClient(TenantStorages[connString]);
                var jobId = jobClient.Enqueue<ITenantJobService>(
                    queueName,
                    service => service.LogMessage(connString)
                );
                Console.WriteLine($"[{DateTime.Now}] Enqueued job {jobId} for {dbName} on queue {queueName}");
            }

            Console.WriteLine($"[{DateTime.Now}] Starting Hangfire servers...");
            await host.StartAsync();
            Console.WriteLine($"[{DateTime.Now}] Tenant Background Services Running. Press ENTER to exit.");
            Console.ReadLine();
            await host.StopAsync();
        }
    }
}