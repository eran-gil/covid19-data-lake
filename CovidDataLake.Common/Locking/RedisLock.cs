using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace CovidDataLake.Common.Locking
{
    public class RedisLock : ILock
    {
        private readonly IConnectionMultiplexer _connection;

        public RedisLock(IConnectionMultiplexer connection)
        {
            _connection = connection;
        }

        public async Task<bool> TakeLockAsync(string lockName, TimeSpan lockExpiration)
        {
            var db = _connection.GetDatabase();
            var lockResult = await db.LockTakeAsync(lockName, 1, lockExpiration);
            return lockResult;
        }

        public async Task<bool> ReleaseLockAsync(string lockName)
        {
            var db = _connection.GetDatabase();
            var lockResult = await db.LockReleaseAsync(lockName, 0);
            return lockResult;
        }
    }
}
