using System;
using System.Threading.Tasks;

namespace CovidDataLake.Pubsub.Kafka.Orchestration
{
    public interface IOrchestrator : IDisposable
    {
        Task StartOrchestration();
    }
}
