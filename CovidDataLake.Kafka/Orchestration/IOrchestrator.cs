using System;
using System.Threading.Tasks;

namespace CovidDataLake.Kafka.Orchestration
{
    public interface IOrchestrator : IDisposable
    {
        Task StartOrchestration();
    }
}
