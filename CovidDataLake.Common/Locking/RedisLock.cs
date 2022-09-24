using StackExchange.Redis;

namespace CovidDataLake.Common.Locking
{
    public class RedisLock : ILock
    {
        private readonly IDatabase _database;

        public RedisLock(IConnectionMultiplexer connection)
        {
            _database = connection.GetDatabase();
        }

        public void TakeLock(string lockName, TimeSpan lockExpiration)
        {
            var token = GetToken();
            var lockResult = false;
            while (!lockResult)
            {
                try
                {
                    lockResult = _database.LockTake(lockName, token, lockExpiration);
                }
                catch
                {
                    // ignored
                }
                if (!lockResult)
                    Thread.Sleep(10);
            }
        }

        public void ReleaseLock(string lockName)
        {
            var token = GetToken();

            var lockResult = false;
            while (!lockResult)
            {
                try
                {
                    lockResult = _database.LockRelease(lockName, token);
                }
                catch
                {
                    // ignored
                }
            }
        }

        public void ExtendLock(string lockName, TimeSpan lockExpiration)
        {
            var token = GetToken();

            var lockResult = false;
            while (!lockResult)
            {
                try
                {
                    lockResult = _database.LockExtend(lockName, token, lockExpiration);
                }
                catch
                {
                    // ignored
                }
            }
        }

        private string GetToken()
        {
            var token = Environment.MachineName;
            return token;
        }
    }
}
