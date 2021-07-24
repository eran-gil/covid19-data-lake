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
        private const string RedisLockKey = ":ROOT_INDEX_LOCK:";
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
                var sortedSetEntries = indexRows.Select(CreateSortedSetEntryFromColumnMapping).ToArray();
                await db.LockTakeAsync(RedisLockKey, 1, _lockTimeSpan);
                await db.SortedSetAddAsync(redisKey, sortedSetEntries);
                await db.LockReleaseAsync(RedisLockKey, 0);
            }
        }

        private static SortedSetEntry CreateSortedSetEntryFromColumnMapping(RootIndexRow mapping)
        {
            return new(mapping.FileName, mapping.Max);
        }

        public async Task<string> GetFileNameForColumnAndValue(string column, ulong val)
        {
            var db = _connection.GetDatabase();
            //todo: need to test what happens when there's no index yet for column or for value...
            var redisKey = GetRedisKeyForColumn(column);
            await db.LockTakeAsync(RedisLockKey, 1, _lockTimeSpan);
            var indexFileValue = (await db.SortedSetRangeByScoreAsync(redisKey, val, take: 1)).FirstOrDefault();
            await db.LockReleaseAsync(RedisLockKey, 0);
            if (indexFileValue == default) return null;
            var indexFileName = indexFileValue.ToString();
            return indexFileName;
        }

        private static string GetRedisKeyForColumn(string column)
        {
            return $"ROOT_INDEX::{column}";
        }
    }
}