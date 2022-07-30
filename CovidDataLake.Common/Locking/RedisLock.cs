using System;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace CovidDataLake.Common.Locking
{
    public class RedisLock : ILock
    {
        private readonly IConnectionMultiplexer _connection;
        private readonly IDatabase _database;

        public RedisLock(IConnectionMultiplexer connection)
        {
            _connection = connection;
            _database = _connection.GetDatabase();
        }

        public async Task<bool> TakeLockAsync(string lockName, TimeSpan lockExpiration)
        {
            var token = GetToken();
            var lockResult = await _database.LockTakeAsync(lockName, token, lockExpiration);
            return lockResult;
        }

        public async Task<bool> ReleaseLockAsync(string lockName)
        {
            var token = GetToken();
            var lockResult = await _database.LockReleaseAsync(lockName, token);
            return lockResult;
        }

        private string GetToken()
        {
            var token = Environment.MachineName;
            return token;
        }
    }
}
