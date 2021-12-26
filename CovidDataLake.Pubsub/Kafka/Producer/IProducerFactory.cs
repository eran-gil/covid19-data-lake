namespace CovidDataLake.Pubsub.Kafka.Producer
{
    public interface IProducerFactory
    {
        IProducer CreateProducer(string clientId);
    }
}
