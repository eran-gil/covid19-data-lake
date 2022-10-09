using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace CovidDataLake.Pubsub.Kafka.Consumer
{
    public interface IConsumer : IDisposable
    {
        void Subscribe(string topic);
        Task Consume(Func<IReadOnlyCollection<string>, Task> handleMessages, CancellationToken cancellationToken);
    }
}
