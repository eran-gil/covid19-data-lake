using CovidDataLake.Kafka.Producer.Configuration;

namespace CovidDataLake.Kafka.Producer
{
    public interface IProducerFactory
    {
        IProducer CreateProducer(string clientId);
    }
}