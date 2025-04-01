namespace HangfireWindowsService.models { 
    public class TenantConfig
    {
        public string ConnectionString { get; set; }
        public int IntervalSeconds { get; set; }
    }
}