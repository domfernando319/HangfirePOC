using System;
using System.Data;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading.Tasks;
using Hangfire;
using Hangfire.MySql;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Hangfire.AspNetCore;
using MySqlConnector;

namespace HangfireService {
    public interface ITenantService {
        Task EnsureDatabaseAndTableExist(string connectionString);
    }

    public class TenantService : ITenantService {
        public async Task EnsureDatabaseAndTableExist(string connectionString) {
            try{
                using var connection = new MySqlConnection(connectionString);
                await connection.OpenAsync();

                string createTableQuery = @"
                    CREATE TABLE IF NOT EXISTS Message (
                        Id INT AUTO_INCREMENT PRIMARY KEY,
                        Message TEXT NOT NULL,
                        CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );";
                using var cmd = new MySqlCommand(createTableQuery, connection);
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
                using var conn = new MySqlConnection(connectionString);
                await conn.OpenAsync();

                string insertQuery = "INSERT INTO Message (Message) VALUES (@message)";
                using var cmd = new MySqlCommand(insertQuery, conn);
                cmd.Parameters.AddWithValue("@message", $"Logged at {DateTime.Now}");

                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine($"[{DateTime.Now}] SUCCESS: Message inserted into {conn.Database}");

                // ***Schedule the next execution dynamically using tenant specific storage
                var interval = Program.Databases[connectionString];
                _jobClients[connectionString].Schedule<ITenantJobService>(
                    service => service.LogMessage(connectionString), 
                    TimeSpan.FromSeconds(interval)
                );
            } catch (Exception e) {
                Console.WriteLine($"ERROR: {e.Message}");
            }
        }
    }

    static class Program {
        public static readonly Dictionary<string, int> Databases = new()
        {
            { "Server=localhost;Database=HangfireDB1;User=root;Password=0319;", 10 }, // 1-second interval
            { "Server=localhost;Database=HangfireDB2;User=root;Password=0319;", 30 }  // 5-second interval
        };

        public static readonly Dictionary<string, MySqlStorage> TenantStorages = new();

        // Helper method to extract database name from connection string
        private static string GetDatabaseNameFromConnectionString(string connectionString)
        {
            var builder = new MySqlConnectionStringBuilder(connectionString);
            return builder.Database;
        }

        static async Task Main() {
            using var host = Host.CreateDefaultBuilder()
                .ConfigureServices((context, services) => {
                    services.AddSingleton<ITenantService, TenantService>();
                    services.AddTransient<ITenantJobService, TenantJobService>();

                    var jobClients = new Dictionary<string, IBackgroundJobClient>();
                    //Configure Hangfire with seperate storage per tenant
                    foreach (var (connectionString, _) in Databases) {
                        var storageOptions = new MySqlStorageOptions {
                            QueuePollInterval = TimeSpan.FromSeconds(1),
                            JobExpirationCheckInterval = TimeSpan.FromHours(1),
                            PrepareSchemaIfNecessary = true
                        };

                        var storage = new MySqlStorage(connectionString, storageOptions);
                        TenantStorages[connectionString] = storage;
                        jobClients[connectionString] = new BackgroundJobClient(storage);

                        string dbName = GetDatabaseNameFromConnectionString(connectionString);
                    
                        // Configure Hangfire with specific storage
                        services.AddHangfire((provider, config) => config
                            .SetDataCompatibilityLevel(CompatibilityLevel.Version_170)
                            .UseSimpleAssemblyNameTypeSerializer()
                            .UseRecommendedSerializerSettings()
                            .UseStorage(storage));

                        // Configure server for this specific tenant
                        services.AddHangfireServer((provider, options) => {
                            options.ServerName = $"Server-{dbName}";
                            options.Queues = new[] { $"queue-{dbName.ToLower()}", "default" };
                            options.WorkerCount = 2; // Adjust based on your needs]
                        });
                    }
                    // Register TenantJobService with job clients
                    services.AddTransient<ITenantJobService>(provider => 
                        new TenantJobService(jobClients));

                    // services.AddHangfire(config => {
                    //     config.SetDataCompatibilityLevel(CompatibilityLevel.Version_170)
                    //         .UseSimpleAssemblyNameTypeSerializer()
                            
                    //         .UseRecommendedSerializerSettings();
                    //     foreach (var connectionString in Databases.Keys) {
                    //         var storage = new MySqlStorage(connectionString, new MySqlStorageOptions{
                    //             QueuePollInterval = TimeSpan.FromSeconds(1), // Make job processing more responsive
                    //             JobExpirationCheckInterval = TimeSpan.FromHours(1) // Ensure jobs don't expire too soon
                    //         });
                    //         config.UseStorage(storage);
                    //     }
                    // });
                    // services.AddHangfireServer();
                    // services.AddTransient<IBackgroundJobClient, BackgroundJobClient>(); // Register Hangfire Job Client
                })
                .Build();

            using var scope = host.Services.CreateScope();
            var tenantService = scope.ServiceProvider.GetRequiredService<ITenantService>();
            var tenantJobService = scope.ServiceProvider.GetRequiredService<ITenantJobService>();
            var backgroundJobClient = scope.ServiceProvider.GetRequiredService<IBackgroundJobClient>();

            // Initialize databases and schedule initial jobs
            foreach (var (connString, _) in Databases) {
                await tenantService.EnsureDatabaseAndTableExist(connString);
                
                // Use tenant-specific job client directly from TenantStorages
                var jobClient = new BackgroundJobClient(TenantStorages[connString]);
                jobClient.Enqueue<ITenantJobService>(
                    service => service.LogMessage(connString)
                );
            }

            Console.WriteLine("Tenant Background Services Running. Press ENTER to exit.");
            await host.RunAsync();
        }
    }
}