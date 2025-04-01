using Hangfire.SqlServer;
using System.Data.SqlClient;
using System;
using System.Data;
using System.Diagnostics;
using Serilog;
using Serilog.Sinks.File;
using System.ServiceProcess;
using System.Collections.Generic;


namespace HangfireWindowsService.services {
    public interface ITenantService {
        Task EnsureDatabaseAndTableExist(string connectionString, bool isWindowsService);
    }

    public class TenantService : ITenantService {
        public async Task EnsureDatabaseAndTableExist(string connectionString, bool isWindowsService) {
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
                if (!isWindowsService) {
                    Console.WriteLine($"SUCCESS: Table 'Message' ensured in database: {connection.Database}");
                }
            } catch (Exception e) {
                if (!isWindowsService) {
                    Console.WriteLine($"ERROR: {e.Message}");
                }
            }
        }
    }
}