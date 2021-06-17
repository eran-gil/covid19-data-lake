namespace CovidDataLake.DAL.Pubsub
{
    public interface IProducerFactory
    {
        IProducer CreateProducer(string clientId);
    }
}