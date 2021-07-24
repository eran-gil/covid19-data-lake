using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing
{
    class AmazonRootIndexFileAccess : IRootIndexFileAccess
    {
        private readonly IRootIndexCache _cache;

        public AmazonRootIndexFileAccess(IRootIndexCache cache)
        {
            _cache = cache;
        }

        public async Task UpdateColumnRanges(RootIndexColumnMappings columnMappings)
        {
            //todo: create root index update/replace for amazon
            await _cache.UpdateColumnRanges(columnMappings);
        }

        public async Task<string> GetFileNameForColumnAndValue(string column, ulong val)
        {
            var cached = await _cache.GetFileNameForColumnAndValue(column, val);
            if (cached != null) return cached;
            Console.WriteLine();
            //TODO: handle missing stuff
            return null;
        }

        
    }
}