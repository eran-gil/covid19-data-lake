using System;
using System.Threading;
using System.Threading.Tasks;

namespace CovidDataLake.Kafka.Consumer
{
    public interface IConsumer : IDisposable
    {
        void Subscribe(string topic);
        Task Consume(Func<string, Task> handleMessage, CancellationToken cancellationToken);
    }
}