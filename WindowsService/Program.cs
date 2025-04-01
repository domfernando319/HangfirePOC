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
using Microsoft.Extensions.Configuration;
using System.Configuration;
using HangfireWindowsService.models;
using HangfireWindowsService.services;

namespace HangfireWindowsService {
    static class Program
    {
        static void Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            var service = new HangfireService(configuration);

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