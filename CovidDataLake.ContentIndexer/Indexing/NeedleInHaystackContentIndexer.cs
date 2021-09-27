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
            var columns = tableWrapper.GetColumns();
            var hashedColumns = HashColumnValues(columns).ToList(); //todo: no list

            var valuesToFilesMapping = hashedColumns.ToDictionary(column => column.Key, GetFileMappingForColumn);
            foreach (var columnMapping in valuesToFilesMapping)
            {
                await UpdateIndexWithColumnMapping(tableWrapper, columnMapping);
            }
            
        }

        private async Task UpdateIndexWithColumnMapping(IFileTableWrapper tableWrapper, KeyValuePair<string, IEnumerable<KeyValuePair<ulong, string>>> columnMapping)
        {
            var (columnName, valuesMapping) = columnMapping;
            var fileGroups = valuesMapping.GroupBy(kvp => kvp.Value);

            var updateIndexTasks = fileGroups
                .Select(group => WriteValuesGroupToFile(group, tableWrapper.Filename))
                .ToArray();
            await Task.WhenAll(updateIndexTasks);
            foreach (var updateIndexTask in updateIndexTasks)
            {
                updateIndexTask.Result.AsParallel().ForAll(row => row.ColumnName = columnName);
            }

            var columnUpdate = new RootIndexColumnUpdate
            {
                ColumnName = columnName,
                Rows = updateIndexTasks.SelectMany(t => t.Result)
            };
            var columnUpdateSet = new SortedSet<RootIndexColumnUpdate> {columnUpdate};
            await _rootIndexAccess.UpdateColumnRanges(columnUpdateSet);
        }

        private Dictionary<string, IEnumerable<ulong>> HashColumnValues(IEnumerable<KeyValuePair<string, IEnumerable<string>>> columns)
        {
            var hashedColumns = new Dictionary<string, IEnumerable<ulong>>();
            foreach (var (columnId, columnValues) in columns)
            {
                hashedColumns[columnId] = columnValues.ToList().Select(s => _hasher.HashStringToUlong(s));
            }

            return hashedColumns;
        }

        private async Task<IEnumerable<RootIndexRow>> WriteValuesGroupToFile(IGrouping<string, KeyValuePair<ulong, string>> fileGroup,
            string originFilename)
        {
            return await _indexFileWriter.UpdateIndexFileWithValues(
                fileGroup.Select(kvp => kvp.Key).ToList(),
                fileGroup.Key, originFilename);
        }

        private IEnumerable<KeyValuePair<ulong, string>> GetFileMappingForColumn(KeyValuePair<string, IEnumerable<ulong>> column)
        {
            var mapping = column.Value.Select(async val =>
                    new KeyValuePair<ulong, string>(val,
                        await _rootIndexAccess.GetFileNameForColumnAndValue(column.Key, val)))
                .Select(t => t.Result);
            return mapping;
        }
    }
}