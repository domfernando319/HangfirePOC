using System.Data.SqlClient;
using Hangfire;
using HangfireWindowsService.models;
using Microsoft.Extensions.Configuration;

namespace HangfireWindowsService.services {
      public interface ITenantJobService {
        Task LogMessage(string connectionString, bool isWindowsService);
    }
    public class TenantJobService : ITenantJobService {
        private readonly Dictionary<string, IBackgroundJobClient> _jobClients;
        private readonly IConfiguration _configuration;

        // Modified constructor to accept tenant-specific job clients
        public TenantJobService(Dictionary<string, IBackgroundJobClient> jobClients, IConfiguration configuration) {
            _jobClients = jobClients;
            _configuration = configuration;
        }
        public async Task LogMessage(string connectionString, bool isWindowsService) {
            try {
                using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync();

                string insertQuery = "INSERT INTO [dbo].[Message] ([Message]) VALUES (@message)";
                using var cmd = new SqlCommand(insertQuery, conn);
                cmd.Parameters.AddWithValue("@message", $"Logged at {DateTime.Now}");
                await cmd.ExecuteNonQueryAsync();
                if (!isWindowsService) {
                    Console.WriteLine($"[{DateTime.Now}] SUCCESS: Message inserted into {conn.Database}");
                }

                // ***Schedule the next execution dynamically using tenant specific storage
                var tenants = _configuration.GetSection("Tenants").Get<Dictionary<string, TenantConfig>>();
                var tenantConfig = tenants?[HangfireService.GetDatabaseNameFromConnectionString(connectionString)];
                var interval = tenantConfig?.IntervalSeconds ?? 60;
                var dbName = HangfireService.GetDatabaseNameFromConnectionString(connectionString);
                var queueName = $"queue-{dbName.ToLower()}";
                var jobId = _jobClients[connectionString].Schedule<ITenantJobService>(
                    queueName,
                    service => service.LogMessage(connectionString, isWindowsService), 
                    TimeSpan.FromSeconds(interval)
                );
                if (!isWindowsService) {
                    Console.WriteLine($"[{DateTime.Now}] Scheduled job {jobId} for {dbName} on queue {queueName}");
                }
            } catch (Exception e) {
                if (!isWindowsService) {
                    Console.WriteLine($"ERROR: {e.Message}");
                }
            }
        }
    }

}