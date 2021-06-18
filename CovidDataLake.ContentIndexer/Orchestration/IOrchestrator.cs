using System;

namespace CovidDataLake.ContentIndexer.Orchestration
{
    public interface IOrchestrator : IDisposable
    {
        void StartOrchestration();
    }
}