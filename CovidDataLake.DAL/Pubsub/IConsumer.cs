using System;

namespace CovidDataLake.DAL.Pubsub
{
    public interface IConsumer : IDisposable
    {
        void Subscribe(string topic);
        void Consume(Action<string> handleMessage);
    }
}