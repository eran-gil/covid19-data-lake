using System.Collections.Generic;

namespace CovidDataLake.Pubsub.Kafka.Producer.Configuration
{
    public class KafkaProducerConfiguration
    {
        public IEnumerable<KafkaInstance> Instances { get; set; }
        public string Topic { get; set; }
    }
}
