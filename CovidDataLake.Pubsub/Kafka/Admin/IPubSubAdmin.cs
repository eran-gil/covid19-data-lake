namespace CovidDataLake.Pubsub.Kafka.Admin
{
    public interface IPubSubAdmin
    {
        IEnumerable<string> GetTopicNames();
        Task DeleteTopicAsync(IEnumerable<string> topics);
    }
}
