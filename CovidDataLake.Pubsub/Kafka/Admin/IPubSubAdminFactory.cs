namespace CovidDataLake.Pubsub.Kafka.Admin
{
    public interface IPubSubAdminFactory
    {
        IPubSubAdmin CreateAdminClient(string clientId);
    }
}
