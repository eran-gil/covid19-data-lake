using System;
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
            var columnUpdates = new SortedSet<RootIndexColumnUpdate>();
            foreach (var column in allColumns)
            {
                var fileMapping = await GetFileMappingForColumn(column);
                var columnUpdate = await UpdateIndexWithColumnMapping(column.Key, fileMapping);
                columnUpdates.Add(columnUpdate);
            }
            await _rootIndexAccess.UpdateColumnRanges(columnUpdates);

        }

        private async Task<IEnumerable<KeyValuePair<string, IAsyncEnumerable<RawEntry>>>> GetColumnValuesFromTableWrapper(IFileTableWrapper tableWrapper)
        {
            try
            {
                var columns = await tableWrapper.GetColumns();
                return columns;
            }
            catch (Exception)
            {
                return Enumerable.Empty<KeyValuePair<string, IAsyncEnumerable<RawEntry>>>();
            }


        }

        private async Task<RootIndexColumnUpdate> UpdateIndexWithColumnMapping(string columnName, IDictionary<string, List<RawEntry>> indexFilesMapping)
        {

            var updateIndexTasks = indexFilesMapping.AsParallel().Select(WriteValuesGroupToFile).ToArray();
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

        private async Task<IEnumerable<RootIndexRow>> WriteValuesGroupToFile(KeyValuePair<string, List<RawEntry>> fileGroup)
        {
            var (indexFileName, values) = fileGroup; 
            var orderedValues = values.OrderBy(kvp=> kvp.Value).ToList();
            var entries = orderedValues.GroupBy(entry => entry.Value).OrderBy(g => g.Key).Select(valuesGroup => valuesGroup.Aggregate(MergeEntries)).ToList();
            return await _indexFileWriter.UpdateIndexFileWithValues(entries, indexFileName);
        }

        private static RawEntry MergeEntries(RawEntry v1, RawEntry v2)
        {
            v1.OriginFilenames.AddRange(v2.OriginFilenames);
            return v1;
        }

        private async Task<IDictionary<string, List<RawEntry>>> GetFileMappingForColumn(KeyValuePair<string, IAsyncEnumerable<RawEntry>> column)
        {
            var (columnName, columnValues) = column;
            //todo: lock root index file and download
            //todo: then pass index file in some way to the get file name and value
            var mapping = new ConcurrentDictionary<string, List<RawEntry>>();
            var lockTask = _rootIndexAccess.EnterBatch();
            lockTask.Wait();
            await Parallel.ForEachAsync(columnValues, async (entry, _) =>
            {
                var indexFileName = await _rootIndexAccess.GetFileNameForColumnAndValue(columnName, entry.Value);
                mapping.AddOrUpdate(indexFileName, new List<RawEntry> { entry }, (_, value) =>
                {
                    value.Add(entry);
                    return value;
                });
            });
            await _rootIndexAccess.ExitBatch();

            return mapping;
        }
    }
}
