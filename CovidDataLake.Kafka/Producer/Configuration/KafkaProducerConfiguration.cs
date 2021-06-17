using System;
using System.Collections.Generic;
using System.Text;

namespace CovidDataLake.Kafka.Producer.Configuration
{
    public class KafkaProducerConfiguration
    {
        public IEnumerable<KafkaInstance> Instances { get; set; }
    }
}
