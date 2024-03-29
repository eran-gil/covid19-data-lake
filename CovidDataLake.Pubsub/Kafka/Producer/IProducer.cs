﻿namespace CovidDataLake.Pubsub.Kafka.Producer
{
    public interface IProducer : IDisposable
    {
        Task<bool> SendMessage(string filename);
    }
}
