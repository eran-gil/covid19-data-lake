namespace CovidDataLake.DAL.Pubsub
{
    public class KafkaProducerFactory : IProducerFactory
    {
        public IProducer CreateProducer(string producerId)
        {
            const string servers = "localhost:9092";
            return new KafkaProducer(servers, producerId);
        }
    }
}
