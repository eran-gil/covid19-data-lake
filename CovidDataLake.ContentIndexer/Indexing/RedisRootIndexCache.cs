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
    public class RedisRootIndexCache : IRootIndexCache
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
                var redisLockKey = GetRedisLockKeyForColumn(columnUpdate.ColumnName);
                await _lockMechanism.TakeLockAsync(redisLockKey, _lockTimeSpan);

                async void UpdateRowInCache(RootIndexRow row) => await UpdateRowCacheInRedis(db, row);

                columnUpdate.Rows.AsParallel().ForAll(UpdateRowInCache);
                await _lockMechanism.ReleaseLockAsync(redisLockKey);
            }
        }

        private static async Task UpdateRowCacheInRedis(IDatabaseAsync db, RootIndexRow row)
        {
            var redisSetKey = GetRedisKeyForColumn(row.ColumnName);
            var redisFilesToValuesKey = GetRedisFilesToValuesKeyForColumn(row.ColumnName);
            var redisValuesToFilesKey = GetRedisValuesToFilesKeyForColumn(row.ColumnName);
            var currentMaxValue = await db.HashGetAsync(redisFilesToValuesKey, row.FileName);
            if (currentMaxValue != default)
            {
                await db.HashDeleteAsync(redisValuesToFilesKey, currentMaxValue);
                await db.SortedSetRemoveAsync(redisSetKey, currentMaxValue);
            }

            await db.HashSetAsync(redisValuesToFilesKey, row.FileName, row.Max);
            await db.HashSetAsync(redisValuesToFilesKey, row.Max, row.FileName);
            await db.SortedSetAddAsync(redisSetKey, row.Max, 0);
        }

        public async Task<string> GetFileNameForColumnAndValue(string column, string val)
        {
            var db = _connection.GetDatabase();
            var redisSetKey = GetRedisKeyForColumn(column);
            var redisValuesToFilesKey = GetRedisValuesToFilesKeyForColumn(column);
            var redisLockKey = GetRedisLockKeyForColumn(column);
            await _lockMechanism.TakeLockAsync(redisLockKey, _lockTimeSpan);
            var indexFileMaxValue = (await db.SortedSetRangeByValueAsync(redisSetKey, val, take: 1)).FirstOrDefault();
            var indexFile = await db.HashGetAsync(redisValuesToFilesKey, indexFileMaxValue);
            await _lockMechanism.ReleaseLockAsync(redisLockKey);
            if (indexFile == default) return null;
            var indexFileName = indexFile.ToString();
            return indexFileName;
        }

        private static string GetRedisKeyForColumn(string column)
        {
            return $"{RedisKeyPrefix}{column}";
        }

        private static string GetRedisValuesToFilesKeyForColumn(string column)
        {
            return $"{RedisValuesToFilesHashMapKey}{column}";
        }

        private static string GetRedisFilesToValuesKeyForColumn(string column)
        {
            return $"{RedisFilesToValuesHashMapKey}{column}";
        }

        private static string GetRedisLockKeyForColumn(string column)
        {
            return $"{RedisLockKeyPrefix}{column}";
        }
    }
}
