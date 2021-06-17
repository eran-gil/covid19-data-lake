using System.Collections.Generic;

namespace CovidDataLake.Kafka.Consumer.Configuration
{
    public class KafkaConsumerConfiguration
    {
        public IEnumerable<KafkaInstance> Instances { get; set; }
        public string ClientId { get; set; }
        public string GroupId { get; set; }
    }

    
}
