using System.Collections.Generic;

namespace CovidDataLake.Kafka.Producer.Configuration
{
    public class KafkaProducerConfiguration
    {
        public IEnumerable<KafkaInstance> Instances { get; set; }
    }
}
