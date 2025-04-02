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
            if (args.Length > 0) {
                if (args[0].Equals("/Install", StringComparison.OrdinalIgnoreCase)) {
                    InstallService();
                    return;
                } else if (args[0].Equals("/Uninstall", StringComparison.OrdinalIgnoreCase)) {
                    UninstallService();
                    return;
                }
            }
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

        static void InstallService() {
            try {
                string exePath = Process.GetCurrentProcess().MainModule.FileName;
                ProcessStartInfo processInfo = new ProcessStartInfo {
                    FileName = "sc.exe",
                    Arguments = $"create HangfireTenantService binPath= \"{exePath}\" DisplayName= \"Hangfire Tenant Service\" start= auto",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                using (Process process = Process.Start(processInfo))
                {
                    process.WaitForExit();
                    string output = process.StandardOutput.ReadToEnd();
                    string error = process.StandardError.ReadToEnd();

                    if (process.ExitCode == 0)
                    {
                        Console.WriteLine("Service installed successfully: " + output);
                    }
                    else
                    {
                        throw new Exception("Failed to install service: " + error);
                    }
                }

                // Start the service after installation
                Process.Start("sc.exe", "start HangfireTenantService").WaitForExit();
                
            } catch (Exception e) {
                Console.WriteLine($"Failed to install service: {e.Message}");
            }
        }

        static void UninstallService()
        {
            try
            {
               // Stop the service first
                Process.Start("sc.exe", "stop HangfireTenantService").WaitForExit();
                ProcessStartInfo processInfo = new ProcessStartInfo
                {
                    FileName = "sc.exe",
                    Arguments = "delete HangfireTenantService",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                using (Process process = Process.Start(processInfo))
                {
                    process.WaitForExit();
                    string output = process.StandardOutput.ReadToEnd();
                    string error = process.StandardError.ReadToEnd();

                    if (process.ExitCode == 0)
                    {
                        Console.WriteLine("Service uninstalled successfully: " + output);
                    }
                    else
                    {
                        throw new Exception("Failed to uninstall service: " + error);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to uninstall service: {e.Message}");
            }
        }
    }
}