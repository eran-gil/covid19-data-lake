using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace CovidDataLake.ContentIndexer.Indexing
{
    class RedisRootIndexCache : IRootIndexCache
    {
        private readonly IConnectionMultiplexer _connection;

        public RedisRootIndexCache(IConnectionMultiplexer connection)
        {
            _connection = connection;
        }

        public Task UpdateRanges(object mapping)
        {
            throw new System.NotImplementedException();
        }

        public async Task<string> GetFileNameForColumnAndValue(string column, ulong val)
        {
            var db = _connection.GetDatabase();
            //todo: need to test what happens when there's no index yet for column or for value...
            var indexFileValue = (await db.SortedSetRangeByScoreAsync($"ROOT_INDEX::{column}", val, take: 1)).FirstOrDefault();
            if (indexFileValue == default) return null;
            var indexFileName = indexFileValue.ToString();
            return indexFileName;
        }
    }
}