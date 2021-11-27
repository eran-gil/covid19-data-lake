using System;
using System.Threading.Tasks;

namespace CovidDataLake.Kafka.Producer
{
    public interface IProducer : IDisposable
    {
        Task<bool> SendMessage(string filename);
    }
}
