using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Common.Hashing;
using CovidDataLake.ContentIndexer.Extraction.TableWrappers;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public class NeedleInHaystackContentIndexer : IContentIndexer
    {
        private readonly IStringHash _hasher;
        private readonly IIndexFileWriter _indexFileWriter;
        private readonly IRootIndexAccess _rootIndexAccess;

        public NeedleInHaystackContentIndexer(IStringHash hasher, IIndexFileWriter indexFileWriter,
            IRootIndexAccess rootIndexAccess)
        {
            _hasher = hasher;
            _indexFileWriter = indexFileWriter;
            _rootIndexAccess = rootIndexAccess;
        }

        public async Task IndexTableAsync(IFileTableWrapper tableWrapper)
        {
            var columns = await tableWrapper.GetColumns();
            var hashedColumns = HashColumnValues(columns).ToList(); //todo: no list

            var valuesToFilesMapping = hashedColumns.ToDictionary(column => column.Key, GetFileMappingForColumn);
            var columnUpdates = new SortedSet<RootIndexColumnUpdate>();
            foreach (var columnMapping in valuesToFilesMapping)
            {
                var columnUpdate = await UpdateIndexWithColumnMapping(tableWrapper, columnMapping);
                columnUpdates.Add(columnUpdate);
            }
            await _rootIndexAccess.UpdateColumnRanges(columnUpdates);

        }

        private async Task<RootIndexColumnUpdate> UpdateIndexWithColumnMapping(IFileTableWrapper tableWrapper, KeyValuePair<string, IAsyncEnumerable<KeyValuePair<ulong, string>>> columnMapping)
        {
            var (columnName, valuesMapping) = columnMapping;
            var fileGroups = valuesMapping.GroupBy(kvp => kvp.Value);

            var updateIndexTasks = await fileGroups
                .Select(group => WriteValuesGroupToFile(group, tableWrapper.Filename))
                .ToArrayAsync();
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

        private Dictionary<string, IAsyncEnumerable<ulong>> HashColumnValues(IEnumerable<KeyValuePair<string, IAsyncEnumerable<string>>> columns)
        {
            var hashedColumns = new Dictionary<string, IAsyncEnumerable<ulong>>();
            foreach (var (columnId, columnValues) in columns)
            {
                hashedColumns[columnId] = columnValues.Select(s => _hasher.HashStringToUlong(s));
            }

            return hashedColumns;
        }

        private async Task<IEnumerable<RootIndexRow>> WriteValuesGroupToFile(IAsyncGrouping<string, KeyValuePair<ulong, string>> fileGroup,
            string originFilename)
        {
            var values = await fileGroup.Select(kvp => kvp.Key).ToListAsync();
            var indexFileName = fileGroup.Key;
            return await _indexFileWriter.UpdateIndexFileWithValues(values, indexFileName, originFilename);
        }

        private IAsyncEnumerable<KeyValuePair<ulong, string>> GetFileMappingForColumn(KeyValuePair<string, IAsyncEnumerable<ulong>> column)
        {
            var mapping = column.Value.Select(async val =>
                    new KeyValuePair<ulong, string>(val,
                        await _rootIndexAccess.GetFileNameForColumnAndValue(column.Key, val)))
                .Select(t => t.Result);
            return mapping;
        }
    }
}