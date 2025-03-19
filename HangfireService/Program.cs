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
                    CREATE TABLE IF NOT EXISTS Log (
                        Id INT AUTO_INCREMENT PRIMARY KEY,
                        Message TEXT NOT NULL,
                        CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );";
                using var cmd = new MySqlCommand(createTableQuery, connection);
                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine($"SUCCESS: Table 'Log' ensured in database: {connection.Database}");
            } catch (Exception e) {
                Console.WriteLine($"ERROR: {e.Message}");
            }
        }
    }

    public interface ITenantJobService {
        Task LogMessage(string connectionString);
    }
    public class TenantJobService : ITenantJobService {
        public async Task LogMessage(string connectionString) {
            try {
                using var conn = new MySqlConnection(connectionString);
                await conn.OpenAsync();

                string insertQuery = "INSERT INTO Log (Message) VALUES (@message)";
                using var cmd = new MySqlCommand(insertQuery, conn);
                cmd.Parameters.AddWithValue("@message", $"Logged at {DateTime.Now}");

                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine($"[{DateTime.Now}] SUCCESS: Log inserted into {conn.Database}");

                // ***Schedule the next execution dynamically using tenant specific storage
                var interval = Program.Databases[connectionString];
                BackgroundJob.Schedule<ITenantJobService>(
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
            { "Server=localhost;Database=HangfireDB1;User=root;Password=0319;", 15 }, // 1-second interval
            { "Server=localhost;Database=HangfireDB2;User=root;Password=0319;", 30 }  // 5-second interval
        };

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
                    
                    //Configure Hangfire with seperate storage per tenant
                    foreach (var (connectionString, _) in Databases) {
                        var storage = new MySqlStorage(connectionString, new MySqlStorageOptions{
                            QueuePollInterval = TimeSpan.FromSeconds(1),
                            JobExpirationCheckInterval = TimeSpan.FromHours(1)
                        });
                        
                        // Get database name from connection string
                        string dbName = GetDatabaseNameFromConnectionString(connectionString);

                        //register Hangfire server for this tenant's storage
                        services.AddHangfire(config => config
                            .SetDataCompatibilityLevel(CompatibilityLevel.Version_170)
                            .UseSimpleAssemblyNameTypeSerializer()
                            .UseRecommendedSerializerSettings()
                            .UseStorage(storage)
                        );

                        // Add dedicated Hangfire server for this tenant
                        services.AddHangfireServer(options => {
                            options.ServerName = $"Server-{dbName}";
                            options.Queues = new[] { $"queue-{dbName.ToLower()}" }; // Unique queue per tenant
                        });
                        
                    }
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

            foreach (var (connString, interval) in Databases) {
                await tenantService.EnsureDatabaseAndTableExist(connString);

                var storage = new MySqlStorage(connString);
                using (new BackgroundJobServerOptions { }.UseStorage(storage)) {
                    GlobalConfiguration.Configuration.UseStorage(storage);
                    BackgroundJob.Enqueue<ITenantJobService>(
                        service => service.LogMessage(connString)
                    );
                }
                // ***** Use IBackgroundJobClient instead of static BackgroundJob API
                // backgroundJobClient.Schedule(
                //     () => tenantJobService.LogMessage(connString), 
                //     TimeSpan.FromSeconds(interval)
                // );
                // Static BackgroundJob API
                // BackgroundJob.Schedule<ITenantJobService>(
                //     service => service.LogMessage(connString), 
                //     TimeSpan.FromSeconds(interval)
                // );
            }

            Console.WriteLine("Tenant Background Services Running. Press ENTER to exit.");
            await host.RunAsync();
        }
    }
}