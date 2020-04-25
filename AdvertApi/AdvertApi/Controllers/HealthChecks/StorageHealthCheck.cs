using System;
using System.Threading;
using System.Threading.Tasks;
using AdvertApi.Services;
using Microsoft.Extensions.HealthChecks;

namespace AdvertApi.Controllers.HealthChecks
{
    public class StorageHealthCheck : IHealthCheck
    {
        private readonly IAdvertStorageService _storageService;

        public StorageHealthCheck(IAdvertStorageService storageService)
        {
            _storageService = storageService;
        }

        public async ValueTask<IHealthCheckResult> CheckAsync(CancellationToken cancellationToken = default)
        {
            bool isStorageOk = await _storageService.CheckHealthAsync();

            return HealthCheckResult.FromStatus(isStorageOk ? CheckStatus.Healthy : CheckStatus.Unhealthy, "");
        }
    }
}
