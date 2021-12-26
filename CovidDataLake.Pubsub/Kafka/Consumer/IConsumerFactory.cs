namespace CovidDataLake.Pubsub.Kafka.Consumer
{
    public interface IConsumerFactory
    {
        IConsumer CreateConsumer(string clientId);
    }
}
