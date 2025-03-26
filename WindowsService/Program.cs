using System;
using System.Data;
using System.Diagnostics;
using Serilog;
using Serilog.Sinks.File;
using System.ServiceProcess;
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

namespace HangfireWindowsService {

    // Custom JobActivator to use IServiceProvider
    public class ServiceProviderJobActivator : JobActivator
    {
        private readonly IServiceProvider _serviceProvider;

        public ServiceProviderJobActivator(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        }

        public override object ActivateJob(Type jobType)
        {
            // If the requested type is TenantJobService, resolve ITenantJobService instead
            if (jobType == typeof(TenantJobService))
            {
                return _serviceProvider.GetService(typeof(ITenantJobService))
                    ?? throw new InvalidOperationException($"Could not resolve service for type {typeof(ITenantJobService).FullName}");
            }
            return _serviceProvider.GetService(jobType)
                ?? throw new InvalidOperationException($"Could not resolve service for type {jobType.FullName}");
        }
    }

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
                // Console.WriteLine($"SUCCESS: Table 'Message' ensured in database: {connection.Database}");
            } catch (Exception e) {
                // Console.WriteLine($"ERROR: {e.Message}");
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
                // Console.WriteLine($"[{DateTime.Now}] SUCCESS: Message inserted into {conn.Database}");

                // ***Schedule the next execution dynamically using tenant specific storage
                var interval = HangfireService.Databases[connectionString];
                var dbName = new SqlConnectionStringBuilder(connectionString).InitialCatalog;
                var queueName = $"queue-{dbName.ToLower()}";
                var jobId = _jobClients[connectionString].Schedule<ITenantJobService>(
                    queueName,
                    service => service.LogMessage(connectionString), 
                    TimeSpan.FromSeconds(interval)
                );
                // Console.WriteLine($"[{DateTime.Now}] Scheduled job {jobId} for {dbName} on queue {queueName}");
            } catch (Exception e) {
                // Console.WriteLine($"ERROR: {e.Message}");
            }
        }
    }

    public class HangfireService : ServiceBase {
        private readonly ILogger _logger;
        private IHost _host;
        private IServiceProvider _services;
        private List<IDisposable> _hangfireServers = new();
        public static readonly Dictionary<string, int> Databases = new() {
            { "Server=DUSFSpectre\\SQLEXPRESS;Database=HangfireDB1;User Id=sa;Password=0319;", 10 },
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

        private static readonly Dictionary<string, SqlServerStorage> TenantStorages = new();

        public HangfireService() {
            ServiceName = "HangfireTenantService";
            // Configure Serilog once during static initialization
            _logger = new LoggerConfiguration()
                .MinimumLevel.Information() // Set minimum log level
                .WriteTo.File(
                    path: "C:\\Users\\Dom Fernando\\BermanDS\\HangfirePOC\\WindowsService\\Logs\\log-.txt", // Log file path with date rolling
                    rollingInterval: RollingInterval.Day, // New file each day
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss} [{Level}] {Message}{NewLine}{Exception}") // Custom format
                .CreateLogger();
        }

        private static string GetDatabaseNameFromConnectionString(string connectionString) {
            var builder = new SqlConnectionStringBuilder(connectionString);
            return builder.InitialCatalog;
        }
        // Make this public for console mode access
        // public void Start(bool isWindowsService=true)
        // {
        //     try {
        //         var services = new ServiceCollection();
        //     services.AddSingleton<ITenantService, TenantService>();
        //     // services.AddTransient<ITenantJobService, TenantJobService>();

        //     var jobClients = new Dictionary<string, IBackgroundJobClient>();

        //     foreach (var (connectionString, _) in Databases)
        //     {
        //         var storageOptions = new SqlServerStorageOptions { /* ... */ };
        //         var storage = new SqlServerStorage(connectionString, storageOptions);
        //         TenantStorages[connectionString] = storage;
        //         jobClients[connectionString] = new BackgroundJobClient(storage);

        //         string dbName = GetDatabaseNameFromConnectionString(connectionString);
        //         string queueName = $"queue-{dbName.ToLower()}";

        //         var config = new BackgroundJobServerOptions
        //         {
        //             ServerName = $"Server-{dbName}",
        //             Queues = new[] { queueName },
        //             WorkerCount = 2,
        //             SchedulePollingInterval = TimeSpan.FromSeconds(1)
        //         };

        //         GlobalConfiguration.Configuration
        //             .SetDataCompatibilityLevel(CompatibilityLevel.Version_170)
        //             .UseSimpleAssemblyNameTypeSerializer()
        //             .UseRecommendedSerializerSettings()
        //             .UseStorage(storage);

        //         _hangfireServers.Add(new BackgroundJobServer(config, storage));
        //     }

        //     services.AddTransient<ITenantJobService>(provider => new TenantJobService(jobClients));
        //     _services = services.BuildServiceProvider();
        //     // GlobalConfiguration.Configuration.UseServiceProvider(_services);

        //     Task.Run(async () =>
        //     {
        //         using var scope = _services.CreateScope();
        //         var tenantService = scope.ServiceProvider.GetRequiredService<ITenantService>();

        //         foreach (var (connString, _) in Databases)
        //         {
        //             await tenantService.EnsureDatabaseAndTableExist(connString);
        //             var dbName = GetDatabaseNameFromConnectionString(connString);
        //             var queueName = $"queue-{dbName.ToLower()}";
        //             var jobClient = new BackgroundJobClient(TenantStorages[connString]);
        //             var jobId = jobClient.Enqueue<ITenantJobService>(
        //                 queueName,
        //                 service => service.LogMessage(connString)
        //             );
        //             if (!isWindowsService) {
        //                 Console.WriteLine($"[{DateTime.Now}] Enqueued job {jobId} for {dbName} on queue {queueName}");
        //             }
        //         }

        //         if (!isWindowsService) {Console.WriteLine($"[{DateTime.Now}] Hangfire Service Started");}
        //     });}
        //     catch(Exception e) {
        //         _logger.Error(e, "Error during service startup: {ErrorMessage}", e.Message);
        //     }
        // }
        public void Start(bool isWindowsService = true)
        {
            try
            {
                var services = new ServiceCollection();
                services.AddSingleton<ITenantService, TenantService>();

                var jobClients = new Dictionary<string, IBackgroundJobClient>();
                services.AddSingleton(jobClients); // Register jobClients so Hangfire can resolve Dictionary<string, IBackgroundJobClient> in DI container
                
                services.AddTransient<ITenantJobService, TenantJobService>(); // Register interface and implementation
                // Build the service provider early to use it with Hangfire
                _services = services.BuildServiceProvider();

                foreach (var (connectionString, _) in Databases)
                {
                    var storageOptions = new SqlServerStorageOptions { /* ... */ };
                    var storage = new SqlServerStorage(connectionString, storageOptions);
                    TenantStorages[connectionString] = storage;
                    jobClients[connectionString] = new BackgroundJobClient(storage);

                    string dbName = GetDatabaseNameFromConnectionString(connectionString);
                    string queueName = $"queue-{dbName.ToLower()}";

                    var config = new BackgroundJobServerOptions
                    {
                        ServerName = $"Server-{dbName}",
                        Queues = new[] { queueName },
                        WorkerCount = 2,
                        SchedulePollingInterval = TimeSpan.FromSeconds(1)
                    };

                    // Configure Hangfire to use the DI container
                    GlobalConfiguration.Configuration
                        .SetDataCompatibilityLevel(CompatibilityLevel.Version_170)
                        .UseSimpleAssemblyNameTypeSerializer()
                        .UseRecommendedSerializerSettings()
                        .UseStorage(storage)
                        .UseActivator(new ServiceProviderJobActivator(_services)); // Custom activator

                    _hangfireServers.Add(new BackgroundJobServer(config, storage));
                }

                // Removed: Update jobClients in the DI container after initialization
                // services.AddTransient<ITenantJobService>(provider => new TenantJobService(jobClients));
                // _services = services.BuildServiceProvider(); // Rebuild with updated services

                Task.Run(async () =>
                {
                    using var scope = _services.CreateScope();
                    var tenantService = scope.ServiceProvider.GetRequiredService<ITenantService>();
                    var tenantJobService = scope.ServiceProvider.GetRequiredService<ITenantJobService>();

                    foreach (var (connString, _) in Databases)
                    {
                        await tenantService.EnsureDatabaseAndTableExist(connString);
                        var dbName = GetDatabaseNameFromConnectionString(connString);
                        var queueName = $"queue-{dbName.ToLower()}";
                        var jobClient = new BackgroundJobClient(TenantStorages[connString]);

                        // Enqueue with the interface, resolved by DI
                        var jobId = jobClient.Enqueue<ITenantJobService>(
                            queueName,
                            service => service.LogMessage(connString)
                        );
                        if (!isWindowsService)
                        {
                            Console.WriteLine($"[{DateTime.Now}] Enqueued job {jobId} for {dbName} on queue {queueName}");
                        }
                    }

                    if (!isWindowsService) { Console.WriteLine($"[{DateTime.Now}] Hangfire Service Started"); }
                });
            }
            catch (Exception e)
            {
                _logger.Error(e, "Error during service startup: {ErrorMessage}", e.Message);
            }
        }
        // Make this public for console mode access
        public void Stop()
        {
            foreach (var server in _hangfireServers)
            {
                server.Dispose();
            }
            (_services as IDisposable)?.Dispose();
            // Console.WriteLine($"[{DateTime.Now}] Hangfire Service Stopped");
        }

        protected override void OnStart(string[] args)
        {
            try {Start();}
            catch(Exception e) {
                _logger.Error(e, "Error during service startup: {ErrorMessage}", e.Message);
            }
        }

        protected override void OnStop()
        {
            Stop();
        }
    }

    static class Program
    {
        static void Main(string[] args)
        {
            var service = new HangfireService();

            if (args.Length > 0 && args[0].ToLower() == "console")
            {
                // Run in console mode
                service.Start(false);
                // Console.WriteLine("Running in console mode. Press any key to stop...");
                Console.ReadKey();
                service.Stop();
            }
            else
            {
                // Run as Windows Service
                ServiceBase.Run(service);
            }
        }
    }
}