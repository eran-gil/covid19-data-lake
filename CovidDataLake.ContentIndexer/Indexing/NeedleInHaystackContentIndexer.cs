using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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

        public async Task IndexTableAsync(IFileTableWrapper tableWrapper)
        {
            var columns = await tableWrapper.GetColumns();

            var valuesToFilesMapping = columns.ToDictionary(column => column.Key, GetFileMappingForColumn);
            var columnUpdates = new SortedSet<RootIndexColumnUpdate>();
            foreach (var columnMapping in valuesToFilesMapping)
            {
                var columnUpdate = await UpdateIndexWithColumnMapping(tableWrapper, columnMapping);
                columnUpdates.Add(columnUpdate);
            }
            await _rootIndexAccess.UpdateColumnRanges(columnUpdates);

        }

        private async Task<RootIndexColumnUpdate> UpdateIndexWithColumnMapping(IFileTableWrapper tableWrapper, KeyValuePair<string, IAsyncEnumerable<KeyValuePair<string, string>>> columnMapping)
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

        private async Task<IEnumerable<RootIndexRow>> WriteValuesGroupToFile(IAsyncGrouping<string, KeyValuePair<string, string>> fileGroup,
            string originFilename)
        {
            var values = await fileGroup.Select(kvp => kvp.Key).ToListAsync();
            var indexFileName = fileGroup.Key;
            return await _indexFileWriter.UpdateIndexFileWithValues(values, indexFileName, originFilename);
        }

        private IAsyncEnumerable<KeyValuePair<string, string>> GetFileMappingForColumn(KeyValuePair<string, IAsyncEnumerable<string>> column)
        {
            var (key, value) = column;
            var mapping = value.Select(async val =>
                    new KeyValuePair<string, string>(val,
                        await _rootIndexAccess.GetFileNameForColumnAndValue(key, val)))
                .Select(t => t.Result);
            return mapping;
        }
    }
}
