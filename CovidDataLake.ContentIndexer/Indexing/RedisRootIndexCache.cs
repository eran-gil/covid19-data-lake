using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Indexing.Models;
using Microsoft.Extensions.Caching.Memory;
using StackExchange.Redis;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public class RedisRootIndexCache : IRootIndexCache
    {
        private readonly IConnectionMultiplexer _connection;
        private readonly IMemoryCache _emptyKeysCache;
        private const string RedisKeyPrefix = "ROOT_INDEX_CACHE::";
        private const string RedisFilesToValuesHashMapKey = "ROOT_INDEX_CACHE_FILES_TO_VALUES_MAP::";
        private const string RedisValuesToFilesHashMapKey = "ROOT_INDEX_CACHE_VALUES_TO_FILEES_MAP::";
        private readonly Task<string> _nullResult = Task.FromResult(default(string));
        public RedisRootIndexCache(IConnectionMultiplexer connection, IMemoryCache memoryCache)
        {
            _connection = connection;
            _emptyKeysCache = memoryCache;
        }

        public async Task UpdateColumnRanges(IReadOnlyCollection<RootIndexColumnUpdate> columnMappings)
        {
            var db = _connection.GetDatabase();
            await Parallel.ForEachAsync(columnMappings, async (columnUpdate, token) =>
            {
                await PerformColumnUpdate(columnUpdate, db, token);
            });
        }

        private static async Task PerformColumnUpdate(RootIndexColumnUpdate columnUpdate, IDatabaseAsync db, CancellationToken token)
        {
            await Parallel.ForEachAsync(columnUpdate.Rows, token, async (row, _) => await UpdateRowCacheInRedis(db, row));
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

        public Task<string> GetFileNameForColumnAndValue(string column, string val)
        {
            var db = _connection.GetDatabase();
            var redisSetKey = GetRedisKeyForColumn(column);
            if (_emptyKeysCache.TryGetValue(redisSetKey, out _))
            {
                return _nullResult;
            }
            var redisValuesToFilesKey = GetRedisValuesToFilesKeyForColumn(column);
            var indexFileMaxValue = (SafeGetSortedRange(val, db, redisSetKey)).FirstOrDefault();
            if (indexFileMaxValue.IsNull)
            {
                _emptyKeysCache.Set(redisSetKey, true, TimeSpan.FromMinutes(1));
                return _nullResult;
            }
            var indexFile = db.HashGet(redisValuesToFilesKey, indexFileMaxValue);
            if (indexFile == default) return _nullResult;
            var indexFileName = indexFile.ToString();
            return Task.FromResult(indexFileName);
        }

        private static IEnumerable<RedisValue> SafeGetSortedRange(string val, IDatabase db, string redisSetKey)
        {
            var attempts = 0;
            while (attempts < 5)
            {
                try
                {
                    return db.SortedSetRangeByValue(redisSetKey, val, take: 1);
                }
                catch
                {
                    attempts++;
                }
            }
            return null;
        }

        public Task EnterBatch()
        {
            return Task.CompletedTask;
        }

        public Task ExitBatch(bool shouldUpdate = false)
        {
            return Task.CompletedTask;
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
    }
}
