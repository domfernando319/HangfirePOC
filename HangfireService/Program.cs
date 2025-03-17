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
    static class Program {
        static async Task Main() {
            var databases = new Dictionary<string, int>
            {
                { "Server=localhost;Database=HangfireDB1;User=root;Password=0319;", 1 }, // 1-second interval
                { "Server=localhost;Database=HangfireDB2;User=root;Password=0319;", 5 }  // 5-second interval
            };

            // Need to await for Database and Table check. Ensure databases and Log tables exist
            foreach (var connString in databases.Keys)
            {
                await EnsureDatabaseAndTableExist(connString);
            }

            var cts = new CancellationTokenSource();

            // Start logging in a separate task
            var loggingTask = RunLoggingLoop(databases, cts.Token);

            Console.WriteLine("Logging started. Press ENTER to stop.");
            Console.ReadLine(); // Wait for user to press Enter

            await cts.CancelAsync();
            await loggingTask; // Ensure task completes

            Console.WriteLine("Logging stopped.");

            // var host = Host.CreateDefaultBuilder()
            // .ConfigureServices((context, services) => {
            //     services.AddHangfire(config => {
            //         foreach (var connectionString in databases) {
            //             var storage = new MySqlStorage(connectionString, new MySqlStorageOptions{QueuePollInterval = TimeSpan.FromSeconds(15)}); // Adjust as needed
            //             config.UseStorage(storage);
            //         }
            //     });
            //     services.AddHangfireServer();
            // })
            // .Build();
            
            // using (var scope = host.Services.CreateScope()) {
            //     var recurringJobManager = scope.ServiceProvider.GetRequiredService<IRecurringJobManager>();

            //     for (int i = 0; i < databases.Count; i++)
            //     {
            //         string connString = databases[i];
            //         int interval = (i == 0) ? 1 : 5; // First DB: 1s, Second DB: 5s
            //         string cronExpression = $"*/{interval} * * * * *"; // Every `interval` seconds

            //         recurringJobManager.AddOrUpdate(
            //             $"log_message_{connString.GetHashCode()}",
            //             () => LogMessage(connString),
            //             cronExpression
            //         );
            //     }
            // }
            // Console.WriteLine("Hangfire Jobs Running. Press any key to exit.");
            // Console.ReadKey();
        }

        private static async Task RunLoggingLoop(Dictionary<string, int> databases, CancellationToken token)
        {
            var timers = new Dictionary<string, Stopwatch>();

            // Initialize stopwatches
            foreach (var connString in databases.Keys)
            {
                timers[connString] = Stopwatch.StartNew();
            }

            while (!token.IsCancellationRequested)
            {
                foreach (var (connString, interval) in databases)
                {
                    if (timers[connString].ElapsedMilliseconds >= interval * 1000)
                    {
                        await LogMessage(connString);
                        timers[connString].Restart();
                    }
                }
                await Task.Delay(500, token); // Small delay to avoid excessive CPU usage
            }
        }

        // Ensure database and log table exist
        private static async Task EnsureDatabaseAndTableExist(string connString)
        {
            try
            {
                using var conn = new MySqlConnection(connString);
                await conn.OpenAsync();

                string createTableQuery = @"
                    CREATE TABLE IF NOT EXISTS Log (
                        Id INT AUTO_INCREMENT PRIMARY KEY,
                        Message TEXT NOT NULL,
                        CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );";

                using var cmd = new MySqlCommand(createTableQuery, conn);
                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine($"SUCCESS: Table 'Log' ensured in database: {conn.Database}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR: Error ensuring database/table: {ex.Message}");
            }
        }

        public static async Task LogMessage(string connString)
        {
            try
            {
                using var conn = new MySqlConnection(connString);
                await conn.OpenAsync();

                string insertQuery = "INSERT INTO Log (Message) VALUES (@message)";
                using var cmd = new MySqlCommand(insertQuery, conn);
                cmd.Parameters.AddWithValue("@message", $"Logged at {DateTime.Now}");

                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine($"[{DateTime.Now}] SUCCESS: Log inserted into {conn.Database}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR: Error inserting log: {ex.Message}");
            }
        }
    }
}