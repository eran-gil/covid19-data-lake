using System;

namespace CovidDataLake.Kafka.Consumer
{
    public interface IConsumer : IDisposable
    {
        void Subscribe(string topic);
        void Consume(Action<string> handleMessage);
    }
}