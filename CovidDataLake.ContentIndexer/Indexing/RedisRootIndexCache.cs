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
                var sortedSetEntries = columnUpdate.Rows.Select(CreateSortedSetEntryFromColumnMapping).ToArray();
                await _lockMechanism.TakeLockAsync(redisLockKey, _lockTimeSpan);
                await db.SortedSetAddAsync(redisKey, sortedSetEntries);
                await _lockMechanism.ReleaseLockAsync(redisLockKey);
            }
        }

        public async Task<string> GetFileNameForColumnAndValue(string column, ulong val)
        {
            var db = _connection.GetDatabase();
            //todo: need to test what happens when there's no index yet for column or for value...
            var redisKey = GetRedisKeyForColumn(column);
            var redisLockKey = GetRedisLockKeyForColumn(column);
            await _lockMechanism.TakeLockAsync(redisLockKey, _lockTimeSpan);
            var indexFileValue = (await db.SortedSetRangeByScoreAsync(redisKey, val, take: 1)).FirstOrDefault();
            await _lockMechanism.ReleaseLockAsync(redisLockKey);
            if (indexFileValue == default) return null;
            var indexFileName = indexFileValue.ToString();
            return indexFileName;
        }

        private static SortedSetEntry CreateSortedSetEntryFromColumnMapping(RootIndexRow mapping)
        {
            return new(mapping.FileName, mapping.Max);
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