using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing.NeedleInHaystack.RootIndex
{
    internal class InMemoryRootIndexCache : IRootIndexCache
    {
        private ConcurrentDictionary<string, SortedSet<RootIndexRow>> _cache;

        public InMemoryRootIndexCache()
        {
            _cache = new ConcurrentDictionary<string, SortedSet<RootIndexRow>>();
        }

        public Task UpdateColumnRanges(IReadOnlyCollection<RootIndexColumnUpdate> columnMappings)
        {
            return Task.CompletedTask;
        }

        public Task<string> GetFileNameForColumnAndValue(string column, string val)
        {
            if (!_cache.ContainsKey(column))
            {
                return Task.FromResult(default(string));
            }
            var columnIndex = _cache[column];
            var max = columnIndex.Max;
            var comparedIndexRow = new RootIndexRow { ColumnName = column, Max = val };
            if(max!.CompareTo(comparedIndexRow) < 0)
            {
                return Task.FromResult(default(string));
            }

            var rangeView = columnIndex.GetViewBetween(comparedIndexRow, max);
            var index = rangeView.First();
            return Task.FromResult(index.FileName);
        }

        public Task EnterBatch()
        {
            return Task.CompletedTask;
        }

        public Task ExitBatch(bool shouldUpdate = false)
        {
            return Task.CompletedTask;
        }

        public Task LoadAllEntries(IEnumerable<RootIndexRow> indexRows)
        {
            _cache = new ConcurrentDictionary<string, SortedSet<RootIndexRow>>();
            foreach (var indexRow in indexRows)
            {
                
                _cache.AddOrUpdate(indexRow.ColumnName, _ => new SortedSet<RootIndexRow>{indexRow},
                    (_, current) =>
                    {
                        current.Add(indexRow);
                        return current;
                    });
            }
            return Task.CompletedTask;
        }

    }
}
