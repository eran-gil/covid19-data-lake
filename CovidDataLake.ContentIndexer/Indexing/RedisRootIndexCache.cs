using System;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Indexing.Models;
using StackExchange.Redis;

namespace CovidDataLake.ContentIndexer.Indexing
{
    class RedisRootIndexCache : IRootIndexCache
    {
        private readonly IConnectionMultiplexer _connection;
        private readonly TimeSpan _lockTimeSpan = TimeSpan.FromSeconds(10);
        private const string RedisKeyPrefix = "ROOT_INDEX::";
        private const string RedisLockKeyPrefix = "ROOT_INDEX_LOCK::";
        public RedisRootIndexCache(IConnectionMultiplexer connection)
        {
            _connection = connection;
        }

        public async Task UpdateColumnRanges(RootIndexColumnMappings columnMappings)
        {
            var db = _connection.GetDatabase();
            foreach (var (key, indexRows) in columnMappings)
            {
                var redisKey = GetRedisKeyForColumn(key);
                var redisLockKey = GetRedisLockKeyForColumn(key);
                var sortedSetEntries = indexRows.Select(CreateSortedSetEntryFromColumnMapping).ToArray();
                await db.LockTakeAsync(redisLockKey, 1, _lockTimeSpan);
                await db.SortedSetAddAsync(redisKey, sortedSetEntries);
                await db.LockReleaseAsync(redisLockKey, 0);
            }
        }

        public async Task<string> GetFileNameForColumnAndValue(string column, ulong val)
        {
            var db = _connection.GetDatabase();
            //todo: need to test what happens when there's no index yet for column or for value...
            var redisKey = GetRedisKeyForColumn(column);
            var redisLockKey = GetRedisLockKeyForColumn(column);
            await db.LockTakeAsync(redisLockKey, 1, _lockTimeSpan);
            var indexFileValue = (await db.SortedSetRangeByScoreAsync(redisKey, val, take: 1)).FirstOrDefault();
            await db.LockReleaseAsync(redisLockKey, 0);
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