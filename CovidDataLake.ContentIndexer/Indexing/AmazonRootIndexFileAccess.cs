using System.Threading.Tasks;

namespace CovidDataLake.ContentIndexer.Indexing
{
    class AmazonRootIndexFileAccess : IRootIndexFileAccess
    {
        private readonly IRootIndexCache _cache;

        public AmazonRootIndexFileAccess(IRootIndexCache cache)
        {
            _cache = cache;
        }
        public Task UpdateRanges(object mapping)
        {
            throw new System.NotImplementedException();
        }

        public async Task<string> GetFileNameForColumnAndValue(string column, ulong val)
        {
            var cached = await _cache.GetFileNameForColumnAndValue(column, val);
            if (cached != null) return cached;
            //TODO: handle missing stuff
            return null;
        }
    }
}