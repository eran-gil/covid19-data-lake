using System;
using System.Collections.Generic;
using System.Text;

namespace CovidDataLake.DAL.Pubsub
{
    public class KafkaConsumerFactory : IConsumerFactory
    {
        public KafkaConsumerFactory()
        {
        }

        public IConsumer CreateConsumer(string clientId, string groupId)
        {
            var servers = "localhost:9092";
            return new KafkaConsumer(servers, clientId, groupId);
        }
    }
}
