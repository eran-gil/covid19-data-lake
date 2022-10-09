using StackExchange.Redis;

namespace CovidDataLake.Scripts.Actions
{
    internal class ResetRedisAction : IScriptAction
    {
        private readonly IConnectionMultiplexer _redisConnection;
        public string Name => "reset_redis";

        public ResetRedisAction(IConnectionMultiplexer redisConnection)
        {
            _redisConnection = redisConnection;
        }
        public async Task<bool> Run()
        {
            try
            {
                var redisEndPoint = _redisConnection.GetEndPoints().First();
                var redisServer = _redisConnection.GetServer(redisEndPoint);
                await redisServer.FlushAllDatabasesAsync();
                Console.WriteLine("Cleaned Redis");
            }
            catch
            {
                //ignore
            }

            return true;
        }
    }
}
