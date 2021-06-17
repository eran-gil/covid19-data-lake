using CovidDataLake.Kafka.Consumer.Configuration;

namespace CovidDataLake.Kafka.Consumer
{
    public interface IConsumerFactory
    {
        IConsumer CreateConsumer(string clientId);
    }
}