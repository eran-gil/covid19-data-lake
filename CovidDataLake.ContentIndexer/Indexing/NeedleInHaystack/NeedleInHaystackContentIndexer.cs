using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Extraction.Models;
using CovidDataLake.ContentIndexer.Extraction.TableWrappers;
using CovidDataLake.ContentIndexer.Indexing.Models;
using CovidDataLake.ContentIndexer.Indexing.NeedleInHaystack.RootIndex;

namespace CovidDataLake.ContentIndexer.Indexing.NeedleInHaystack
{
    public class NeedleInHaystackContentIndexer : IContentIndexer
    {
        private readonly NeedleInHaystackIndexWriter _indexFileWriter;
        private readonly IRootIndexAccess _rootIndexAccess;

        public NeedleInHaystackContentIndexer(NeedleInHaystackIndexWriter indexFileWriter,
            IRootIndexAccess rootIndexAccess)
        {
            _indexFileWriter = indexFileWriter;
            _rootIndexAccess = rootIndexAccess;
        }

        public async Task IndexTableAsync(IEnumerable<IFileTableWrapper> tableWrappers)
        {
            var allColumns = GetAllColumns(tableWrappers);
            if (!allColumns.Any())
            {
                return;
            }

            var lockTask = _rootIndexAccess.EnterBatch();
            lockTask.Wait();
            var columnUpdates = await UpdateAllColumns(allColumns);
            await _rootIndexAccess.UpdateColumnRanges(columnUpdates);
            await _rootIndexAccess.ExitBatch(true);
        }

        private static IDictionary<string, IAsyncEnumerable<RawEntry>> GetAllColumns(IEnumerable<IFileTableWrapper> tableWrappers)
        {
            var unifiedColumns = new ConcurrentDictionary<string, IAsyncEnumerable<RawEntry>>();
            var rawColumns = tableWrappers.SelectMany(wrapper => wrapper.GetColumns());

            foreach (var column in rawColumns)
            {
                unifiedColumns.AddOrUpdate(
                    column.Key,
                    column.Value,
                    (_, current) => current.Concat(column.Value)
                );
            }

            return unifiedColumns;
        }

        private async Task<IReadOnlyCollection<RootIndexColumnUpdate>> UpdateAllColumns(IDictionary<string, IAsyncEnumerable<RawEntry>> columns)
        {
            
            var tasks = columns.Select(UpdateColumnIndex).ToList();
            var columnUpdates = await Task.WhenAll(tasks);
            return columnUpdates;
        }

        private async Task<RootIndexColumnUpdate> UpdateColumnIndex(KeyValuePair<string, IAsyncEnumerable<RawEntry>> column)
        {
            //blocking collection of index file names
            //
            var mappedEntries = GetIndexFileNamesForColumns(column);
            var columnUpdate = await UpdateIndexWithColumnMapping(column.Key, mappedEntries);
            return columnUpdate;
        }

        private async IAsyncEnumerable<KeyValuePair<string, RawEntry>> GetIndexFileNamesForColumns(
            KeyValuePair<string, IAsyncEnumerable<RawEntry>> column)
        {
            var (columnName, columnValues) = column;
           await  foreach (var columnValue in columnValues)
            {
                var indexFileName = await _rootIndexAccess.GetFileNameForColumnAndValue(columnName, columnValue.Value);
                var mapping = new KeyValuePair<string, RawEntry>(indexFileName, columnValue);
                yield return mapping;
            }

        }

        private async Task<RootIndexColumnUpdate> UpdateIndexWithColumnMapping(string columnName, IAsyncEnumerable<KeyValuePair<string, RawEntry>> mappedEntries)
        {
            var indexFilesQueues = new Dictionary<string, Channel<RawEntry>>();
            var indexTasks = new List<Task<IEnumerable<RootIndexRow>>>();
            await foreach (var mappedEntry in mappedEntries)
            {
                var (indexFileName, entry) = mappedEntry;
                if (!indexFilesQueues.ContainsKey(indexFileName))
                {
                    indexFilesQueues[indexFileName] = Channel.CreateUnbounded<RawEntry>();
                    var queueEnumerable = indexFilesQueues[indexFileName].Reader.ReadAllAsync();
                    var indexTask = _indexFileWriter.UpdateIndexFileWithValues(columnName, indexFileName, queueEnumerable);
                    indexTasks.Add(indexTask);
                }

                await indexFilesQueues[indexFileName].Writer.WriteAsync(entry);
            }

            FinishAllQueues(indexFilesQueues);

            var rootIndexRows = (await Task.WhenAll(indexTasks))
                .SelectMany(rows => rows)
                .ToList();

            foreach (var rootIndexRow in rootIndexRows)
            {
                rootIndexRow.ColumnName = columnName;
            }

            var columnUpdate = new RootIndexColumnUpdate
            {
                ColumnName = columnName,
                Rows = rootIndexRows
            };
            return columnUpdate;
        }

        private static void FinishAllQueues(IDictionary<string, Channel<RawEntry>> indexFilesQueues)
        {
            foreach (var queue in indexFilesQueues.Values)
            {
                queue.Writer.Complete();
            }
        }
    }
}
