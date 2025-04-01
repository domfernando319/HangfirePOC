using Hangfire;

namespace HangfireWindowsService.services {
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

}