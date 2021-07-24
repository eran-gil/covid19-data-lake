using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Common.Hashing;
using CovidDataLake.ContentIndexer.TableWrappers;

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

        public async Task IndexTable(IFileTableWrapper tableWrapper)
        {
            var columns = tableWrapper.GetColumns();
            var hashedColumns = HashColumnValues(columns);

            var valuesToFilesMapping = hashedColumns.SelectMany(GetFileMappingForColumn);

            var fileGroups = valuesToFilesMapping.GroupBy(kvp => kvp.Value);

            var updateIndexTasks = fileGroups
                .Select(group => WriteValuesGroupToFile(group, tableWrapper.Filename))
                .ToArray();
            await Task.WhenAll(updateIndexTasks);
        }

        private Dictionary<string, IEnumerable<ulong>> HashColumnValues(IEnumerable<KeyValuePair<string, IEnumerable<string>>> columns)
        {
            var hashedColumns = new Dictionary<string, IEnumerable<ulong>>();
            foreach (var (columnId, columnValues) in columns)
            {
                hashedColumns[columnId] = columnValues.Select(s => _hasher.HashStringToUlong(s));
            }

            return hashedColumns;
        }

        private async Task WriteValuesGroupToFile(IGrouping<string, KeyValuePair<ulong, string>> fileGroup,
            string originFilename)
        {
            await _indexFileWriter.UpdateIndexFileWithValues(
                fileGroup.Select(kvp => kvp.Key).ToList(),
                fileGroup.Key, originFilename);
        }

        private IEnumerable<KeyValuePair<ulong, string>> GetFileMappingForColumn(KeyValuePair<string, IEnumerable<ulong>> column)
        {
            var mapping = column.Value.AsParallel().Select(async val =>
                    new KeyValuePair<ulong, string>(val,
                        await _rootIndexAccess.GetFileNameForColumnAndValue(column.Key, val)))
                .Select(t => t.Result);
            return mapping;
        }
    }
}