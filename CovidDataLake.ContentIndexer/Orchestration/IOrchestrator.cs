using System;
using System.Threading.Tasks;

namespace CovidDataLake.ContentIndexer.Orchestration
{
    public interface IOrchestrator : IDisposable
    {
        Task StartOrchestration();
    }
}
