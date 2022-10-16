using CovidDataLake.Common.Logging;
using Microsoft.Extensions.Logging;

namespace CovidDataLake.Common
{
    public static class LoggingExtensions
    {
        public static LoggingStep Step(this ILogger logger, string name, Dictionary<string, object>? properties = null)
        {
            var step = new LoggingStep(name, logger, properties);
            step.Start();
            return step;
        }
    }
}
