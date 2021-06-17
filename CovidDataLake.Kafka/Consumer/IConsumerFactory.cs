namespace CovidDataLake.DAL.Pubsub
{
    public interface IConsumerFactory
    {
        IConsumer CreateConsumer(string clientId);
    }
}