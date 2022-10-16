namespace CovidDataLake.Pubsub.Kafka.Extensions
{
    public static class KafkaInstanceExtensions
    {
        public static string ToConnectionString(this IEnumerable<KafkaInstance> instances)
        {
            var connectionString = instances
                .Select(instance => $"{instance.Host}:{instance.Port}")
                .Aggregate((s1, s2) => $"{s1},{s2}");
            return connectionString;
        }
    }
}
