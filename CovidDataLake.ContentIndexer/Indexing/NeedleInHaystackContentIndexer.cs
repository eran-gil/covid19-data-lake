using System.Collections.Concurrent;
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
            var mergedColumns = MergeColumnsByName(tableWrappers);
            if (!mergedColumns.Any())
            {
                return;
            }
            var columnUpdates = new ConcurrentBag<RootIndexColumnUpdate>();
            var lockTask = _rootIndexAccess.EnterBatch();
            lockTask.Wait();
            await Parallel.ForEachAsync(mergedColumns, async (column, _) =>
                {
                    var columnUpdate = await UpdateColumnIndex(column);
                    columnUpdates.Add(columnUpdate);
                }
            );
            await _rootIndexAccess.UpdateColumnRanges(columnUpdates);
            await _rootIndexAccess.ExitBatch(true);
        }

        private static ConcurrentDictionary<string, IEnumerable<RawEntry>> MergeColumnsByName(IEnumerable<IFileTableWrapper> tableWrappers)
        {
            var unifiedColumns = new ConcurrentDictionary<string, IEnumerable<RawEntry>>();
            var allColumns = tableWrappers
                .SelectMany(wrapper => wrapper.GetColumns());

            foreach (var column in allColumns)
            {
                unifiedColumns.AddOrUpdate(
                    column.Key,
                    column.Value,
                    (_, current) => current.Concat(column.Value)
                );
            }

            return unifiedColumns;
        }

        private async Task<RootIndexColumnUpdate> UpdateColumnIndex(KeyValuePair<string, IEnumerable<RawEntry>> column)
        {
            var fileMapping = await GetFileMappingForColumn(column);
            var columnUpdate = await UpdateIndexWithColumnMapping(column.Key, fileMapping);
            return columnUpdate;
        }

        private async Task<RootIndexColumnUpdate> UpdateIndexWithColumnMapping(string columnName, IDictionary<string, List<RawEntry>> indexFilesMapping)
        {
            var updateResults = new ConcurrentBag<RootIndexRow>();
            await Parallel.ForEachAsync(indexFilesMapping, async (indexFileValues, _) =>
            {
                var updateResult = await WriteValuesGroupToFile(indexFileValues, columnName);
                foreach (var rootIndexRow in updateResult)
                {
                    updateResults.Add(rootIndexRow);
                    rootIndexRow.ColumnName = columnName;
                }

            });

            var columnUpdate = new RootIndexColumnUpdate
            {
                ColumnName = columnName,
                Rows = updateResults
            };
            return columnUpdate;
        }

        private async Task<IEnumerable<RootIndexRow>> WriteValuesGroupToFile(KeyValuePair<string, List<RawEntry>> fileGroup, string columnName)
        {
            var (indexFileName, values) = fileGroup; 
            var entries = values
                .Where(entry => entry != null)
                .GroupBy(entry => entry.Value)
                .OrderBy(g => g.Key)
                .Select(valuesGroup => valuesGroup.Aggregate(MergeEntries))
                .ToList();
            return await _indexFileWriter.UpdateIndexFileWithValues(entries, indexFileName, columnName);
        }

        private static RawEntry MergeEntries(RawEntry v1, RawEntry v2)
        {
            v1.MergeEntries(v2);
            return v1;
        }

        private async Task<IDictionary<string, List<RawEntry>>> GetFileMappingForColumn(KeyValuePair<string, IEnumerable<RawEntry>> column)
        {
            var (columnName, columnValues) = column;
            var mapping = new ConcurrentDictionary<string, List<RawEntry>>();

            await Parallel.ForEachAsync(columnValues, async (entry, _) =>
            {
                var indexFileName = await _rootIndexAccess.GetFileNameForColumnAndValue(columnName, entry.Value);
                mapping.AddOrUpdate(indexFileName, new List<RawEntry> { entry }, (_, value) =>
                {
                    value.Add(entry);
                    return value;
                });
            });

            return mapping;
        }
    }
}
