using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Common.Locking;
using CovidDataLake.ContentIndexer.Configuration;
using CovidDataLake.ContentIndexer.Indexing.Models;
using StackExchange.Redis;

namespace CovidDataLake.ContentIndexer.Indexing
{
    class RedisRootIndexCache : IRootIndexCache
    {
        private readonly IConnectionMultiplexer _connection;
        private readonly ILock _lockMechanism;
        private readonly TimeSpan _lockTimeSpan;
        private const string RedisKeyPrefix = "ROOT_INDEX_CACHE::";
        private const string RedisFilesToValuesHashMapKey = "ROOT_INDEX_CACHE_FILES_TO_VALUES_MAP::";
        private const string RedisValuesToFilesHashMapKey = "ROOT_INDEX_CACHE_VALUES_TO_FILEES_MAP::";
        private const string RedisLockKeyPrefix = "ROOT_INDEX_CACHE_LOCK::";

        public RedisRootIndexCache(IConnectionMultiplexer connection, ILock lockMechanism, RedisIndexCacheConfiguration configuration)
        {
            _connection = connection;
            _lockMechanism = lockMechanism;
            _lockTimeSpan = TimeSpan.FromSeconds(configuration.LockDurationInSeconds);
        }

        public async Task UpdateColumnRanges(SortedSet<RootIndexColumnUpdate> columnMappings)
        {
            var db = _connection.GetDatabase();
            foreach (var columnUpdate in columnMappings)
            {
                var redisKey = GetRedisKeyForColumn(columnUpdate.ColumnName);
                var redisLockKey = GetRedisLockKeyForColumn(columnUpdate.ColumnName);
                await _lockMechanism.TakeLockAsync(redisLockKey, _lockTimeSpan);
                columnUpdate.Rows.AsParallel().ForAll(async row => await UpdateRowCacheInRedis(db, row, redisKey));
                await _lockMechanism.ReleaseLockAsync(redisLockKey);
            }
        }

        private static async Task UpdateRowCacheInRedis(IDatabaseAsync db, RootIndexRow row, string redisKey)
        {
            var currentMaxValue = await db.HashGetAsync(RedisFilesToValuesHashMapKey, row.FileName);
            await db.HashDeleteAsync(RedisValuesToFilesHashMapKey, currentMaxValue);
            await db.SortedSetRemoveAsync(redisKey, currentMaxValue);
            await db.HashSetAsync(RedisFilesToValuesHashMapKey, row.FileName, row.Max);
            await db.HashSetAsync(RedisValuesToFilesHashMapKey, row.Max, row.FileName);
            await db.SortedSetAddAsync(redisKey, row.Max, 0);
        }

        public async Task<string> GetFileNameForColumnAndValue(string column, string val)
        {
            var db = _connection.GetDatabase();
            var redisKey = GetRedisKeyForColumn(column);
            var redisLockKey = GetRedisLockKeyForColumn(column);
            await _lockMechanism.TakeLockAsync(redisLockKey, _lockTimeSpan);
            var indexFileValue = (await db.SortedSetRangeByValueAsync(redisKey, val, take: 1)).FirstOrDefault();
            await _lockMechanism.ReleaseLockAsync(redisLockKey);
            if (indexFileValue == default) return null;
            var indexFileName = indexFileValue.ToString();
            return indexFileName;
        }

        private static string GetRedisKeyForColumn(string column)
        {
            return $"{RedisKeyPrefix}{column}";
        }
        
        private static string GetRedisLockKeyForColumn(string column)
        {
            return $"{RedisLockKeyPrefix}{column}";
        }
    }
}
