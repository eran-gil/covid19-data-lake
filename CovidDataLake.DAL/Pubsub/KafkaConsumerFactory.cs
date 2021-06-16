namespace CovidDataLake.DAL.Pubsub
{
    public class KafkaConsumerFactory : IConsumerFactory
    {
        public KafkaConsumerFactory()
        {
            //TODO: use configuration
        }

        public IConsumer CreateConsumer(string clientId, string groupId)
        {
            var servers = "localhost:9092";
            return new KafkaConsumer(servers, clientId, groupId);
        }
    }
}
