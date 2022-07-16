using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Extraction.Models;
using CovidDataLake.ContentIndexer.Extraction.TableWrappers;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public class NeedleInHaystackContentIndexer : IContentIndexer
    {
        private readonly IIndexFileWriter _indexFileWriter;
        private readonly IRootIndexAccess _rootIndexAccess;

        public NeedleInHaystackContentIndexer(IIndexFileWriter indexFileWriter,
            IRootIndexAccess rootIndexAccess)
        {
            _indexFileWriter = indexFileWriter;
            _rootIndexAccess = rootIndexAccess;
        }

        public async Task IndexTableAsync(IEnumerable<IFileTableWrapper> tableWrappers)
        {
            var allColumns = new Dictionary<string, IAsyncEnumerable<RawEntry>>();
            foreach (var tableWrapper in tableWrappers)
            {
                var columnValues = await GetColumnValuesFromTableWrapper(tableWrapper);
                foreach (var (columnName, values) in columnValues)
                {
                    if (allColumns.ContainsKey(columnName))
                    {
                        allColumns[columnName] = allColumns[columnName].Concat(values);
                    }
                    else
                    {
                        allColumns[columnName] = values;
                    }
                }

            }
            if (!allColumns.Any())
            {
                return;
            }
            var valuesToFilesMapping = allColumns.ToDictionary(column => column.Key, GetFileMappingForColumn);
            var columnUpdates = new SortedSet<RootIndexColumnUpdate>();
            foreach (var columnMapping in valuesToFilesMapping)
            {
                var columnUpdate = await UpdateIndexWithColumnMapping(columnMapping);
                columnUpdates.Add(columnUpdate);
            }
            await _rootIndexAccess.UpdateColumnRanges(columnUpdates);

        }

        private async Task<IEnumerable<KeyValuePair<string, IAsyncEnumerable<RawEntry>>>> GetColumnValuesFromTableWrapper(IFileTableWrapper tableWrapper)
        {
            var columns = await tableWrapper.GetColumns();

            return columns;
        }

        private async Task<RootIndexColumnUpdate> UpdateIndexWithColumnMapping(KeyValuePair<string, IAsyncEnumerable<KeyValuePair<RawEntry, string>>> columnMapping)
        {
            var (columnName, valuesMapping) = columnMapping;
            var fileGroups = valuesMapping.GroupBy(kvp => kvp.Value);
            
            var updateIndexTasks = await fileGroups.Select(WriteValuesGroupToFile).ToArrayAsync();
            await Task.WhenAll(updateIndexTasks);
            foreach (var updateIndexTask in updateIndexTasks)
            {
                updateIndexTask.Result.AsParallel().ForAll(row => row.ColumnName = columnName);
            }

            var columnUpdate = new RootIndexColumnUpdate
            {
                ColumnName = columnName,
                Rows = updateIndexTasks.SelectMany(t => t.Result).ToList()
            };
            return columnUpdate;
        }

        private async Task<IEnumerable<RootIndexRow>> WriteValuesGroupToFile(IAsyncGrouping<string, KeyValuePair<RawEntry, string>> fileGroup)
        {
            var values = await fileGroup.OrderBy(kvp=> kvp.Key.Value).Select(kvp => kvp.Key).ToListAsync();
            values = values.GroupBy(entry => entry.Value).OrderBy(g => g.Key).Select(valuesGroup => valuesGroup.Aggregate(MergeEntries)).ToList();
            var indexFileName = fileGroup.Key;
            return await _indexFileWriter.UpdateIndexFileWithValues(values, indexFileName);
        }

        private static RawEntry MergeEntries(RawEntry v1, RawEntry v2)
        {
            v1.OriginFilenames.AddRange(v2.OriginFilenames);
            return v1;
        }

        private IAsyncEnumerable<KeyValuePair<RawEntry, string>> GetFileMappingForColumn(KeyValuePair<string, IAsyncEnumerable<RawEntry>> column)
        {
            var (key, value) = column;
            var mapping = value.Select(async val =>
                    new KeyValuePair<RawEntry, string>(val,
                        await _rootIndexAccess.GetFileNameForColumnAndValue(key, val.Value)))
                .Select(t => t.Result);
            return mapping;
        }
    }
}
