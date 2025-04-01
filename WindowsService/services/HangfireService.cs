using System.Data.SqlClient;
using System.ServiceProcess;
using Hangfire;
using Hangfire.SqlServer;
using HangfireWindowsService.models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace HangfireWindowsService.services {
    
    public class HangfireService : ServiceBase {
        private readonly ILogger _logger;
        private IServiceProvider _services;
        private List<IDisposable> _hangfireServers = new();
        private readonly IConfiguration _configuration;

        private static readonly Dictionary<string, SqlServerStorage> TenantStorages = new();

        public HangfireService(IConfiguration configuration) {
            ServiceName = "HangfireTenantService";
            _configuration = configuration;
            // Configure Serilog once during static initialization
            _logger = new LoggerConfiguration()
                .MinimumLevel.Information() // Set minimum log level
                .WriteTo.File(
                    path: "C:\\Users\\Dom Fernando\\BermanDS\\HangfirePOC\\WindowsService\\Logs\\log-.txt", // Log file path with date rolling
                    rollingInterval: RollingInterval.Day, // New file each day
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss} [{Level}] {Message}{NewLine}{Exception}") // Custom format
                .CreateLogger();
        }

        public static string GetDatabaseNameFromConnectionString(string connectionString) {
            var builder = new SqlConnectionStringBuilder(connectionString);
            return builder.InitialCatalog;
        }
    
        public void Start(bool isWindowsService = true)
        {
            try
            {
                _logger.Information("Application started");
                var services = new ServiceCollection();
                services.AddSingleton<ITenantService, TenantService>();
                services.AddSingleton(_configuration); // register IConfiguration
                var jobClients = new Dictionary<string, IBackgroundJobClient>();
                services.AddSingleton(jobClients); // Register jobClients so Hangfire can resolve Dictionary<string, IBackgroundJobClient> in DI container
                
                services.AddTransient<ITenantJobService, TenantJobService>(); // Register interface and implementation
                // Build the service provider early to use it with Hangfire
                _services = services.BuildServiceProvider();

                var tenants = _configuration.GetSection("Tenants").Get<Dictionary<string, TenantConfig>>();
                foreach (var (tenantName, tenantConfig) in tenants)
                {
                    var connectionString = tenantConfig.ConnectionString;
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

                    foreach (var (tenantName, tenantConfig) in tenants)
                    {
                        var connString = tenantConfig.ConnectionString;
                        await tenantService.EnsureDatabaseAndTableExist(connString, isWindowsService);
                        var dbName = GetDatabaseNameFromConnectionString(connString);
                        var queueName = $"queue-{dbName.ToLower()}";
                        var jobClient = new BackgroundJobClient(TenantStorages[connString]);

                        // Enqueue with the interface, resolved by DI
                        var jobId = jobClient.Enqueue<ITenantJobService>(
                            queueName,
                            service => service.LogMessage(connString, isWindowsService)
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
}