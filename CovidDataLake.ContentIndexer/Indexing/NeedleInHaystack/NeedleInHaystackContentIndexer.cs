using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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

        private static ConcurrentDictionary<string, IEnumerable<RawEntry>> GetAllColumns(IEnumerable<IFileTableWrapper> tableWrappers)
        {
            var unifiedColumns = new ConcurrentDictionary<string, IEnumerable<RawEntry>>();
            var rawColumns = tableWrappers
                .SelectMany(wrapper => wrapper.GetColumns());

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

        private async Task<ConcurrentBag<RootIndexColumnUpdate>> UpdateAllColumns(ConcurrentDictionary<string, IEnumerable<RawEntry>> columns)
        {
            var columnUpdates = new ConcurrentBag<RootIndexColumnUpdate>();
            await Parallel.ForEachAsync(columns, async (column, _) =>
                {
                    var columnUpdate = await UpdateColumnIndex(column);
                    columnUpdates.Add(columnUpdate);
                }
            );
            return columnUpdates;
        }

        private async Task<RootIndexColumnUpdate> UpdateColumnIndex(KeyValuePair<string, IEnumerable<RawEntry>> column)
        {
            //blocking collection of index file names
            //
            var mappedEntries = GetIndexFileNamesForColumns(column);
            var columnUpdate = await UpdateIndexWithColumnMapping(column.Key, mappedEntries.GetConsumingEnumerable());
            return columnUpdate;
        }

        private BlockingCollection<KeyValuePair<string, RawEntry>> GetIndexFileNamesForColumns(
            KeyValuePair<string, IEnumerable<RawEntry>> column)
        {
            var results = new BlockingCollection<KeyValuePair<string, RawEntry>>();
            ProduceIndexFileMapping(column, results);
            return results;

        }

        private async Task ProduceIndexFileMapping(KeyValuePair<string, IEnumerable<RawEntry>> column,
            BlockingCollection<KeyValuePair<string, RawEntry>> queue)
        {
            var (columnName, columnValues) = column;
            await Parallel.ForEachAsync(columnValues, async (columnValue, token) =>
            {
                var indexFileName = await _rootIndexAccess.GetFileNameForColumnAndValue(columnName, columnValue.Value);
                var mapping = new KeyValuePair<string, RawEntry>(indexFileName, columnValue);
                queue.Add(mapping, token);
            });
            queue.CompleteAdding();
        }

        private async Task<RootIndexColumnUpdate> UpdateIndexWithColumnMapping(string columnName, IEnumerable<KeyValuePair<string, RawEntry>> mappedEntries)
        {
            var indexFilesQueues = new ConcurrentDictionary<string, BlockingCollection<RawEntry>>();
            var indexTasks = new List<Task<IEnumerable<RootIndexRow>>>();
            foreach (var mappedEntry in mappedEntries)
            {
                var (indexFileName, entry) = mappedEntry;
                indexFilesQueues.AddOrUpdate(indexFileName,
                    _ =>
                    {
                        var entries = new BlockingCollection<RawEntry> { entry };
                        var indexTask = Task.Run(async () =>
                            await _indexFileWriter.UpdateIndexFileWithValues(columnName, indexFileName,
                                entries.GetConsumingEnumerable()));
                        indexTasks.Add(indexTask);
                        return entries;
                    },
                    (_, queue) =>
                    {
                        queue.Add(entry);
                        return queue;
                    }
                );
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

        private static void FinishAllQueues(ConcurrentDictionary<string, BlockingCollection<RawEntry>> indexFilesQueues)
        {
            foreach (var queue in indexFilesQueues.Values)
            {
                queue.CompleteAdding();
            }
        }
    }
}
