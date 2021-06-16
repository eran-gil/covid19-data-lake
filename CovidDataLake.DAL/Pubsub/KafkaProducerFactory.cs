namespace CovidDataLake.DAL.Pubsub
{
    public class KafkaProducerFactory : IProducerFactory
    {
        public IProducer CreateProducer(string producerId)
        {
            //TODO: use configuration
            const string servers = "localhost:9092";
            return new KafkaProducer(servers, producerId);
        }
    }
}
