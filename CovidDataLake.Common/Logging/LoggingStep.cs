using Microsoft.Extensions.Logging;

namespace CovidDataLake.Common.Logging
{
    public class LoggingStep : IDisposable
    {
        private readonly string _name;
        private readonly ILogger _logger;
        private readonly Dictionary<string, object>? _scopeData;

        public LoggingStep(string name, ILogger logger, Dictionary<string, object>? scopeData)
        {
            _name = name;
            _logger = logger;
            _scopeData = scopeData;
        }

        public void Start()
        {
            if (_scopeData != null)
            {
                _logger.BeginScope(_scopeData);
            }
            _logger.LogInformation($"{_name}-start");
        }

        public void Dispose()
        {
            _logger.LogInformation($"{_name}-end");
        }
    }
}
