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
            var hashedColumns = new Dictionary<string, IList<ulong>>();
            foreach (var columnId in columns.Keys)
            {
                hashedColumns[columnId] = columns[columnId].Select(s => _hasher.HashStringToUlong(s)).ToList();
            }

            var valuesToFilesMapping = hashedColumns.SelectMany(GetFileMappingForColumn);

            var fileGroups = valuesToFilesMapping.GroupBy(kvp => kvp.Value);

            var updateIndexTasks = fileGroups
                .Select((group) => WriteValuesGroupToFile(group, tableWrapper.Filename))
                .ToArray();
            await Task.WhenAll(updateIndexTasks);
        }

        private async Task WriteValuesGroupToFile(IGrouping<string, KeyValuePair<ulong, string>> fileGroup,
            string originFilename)
        {
            await _indexFileWriter.UpdateIndexFileWithValues(
                fileGroup.Select(kvp => kvp.Key).ToList(),
                fileGroup.Key, originFilename);
        }

        private IList<KeyValuePair<ulong, string>> GetFileMappingForColumn(KeyValuePair<string, IList<ulong>> column)
        {
            var mapping = column.Value.AsParallel().Select(async val =>
                    new KeyValuePair<ulong, string>(val,
                        await _rootIndexAccess.GetFileNameForColumnAndValue(column.Key, val)))
                .Select(t => t.Result).ToList();
            return mapping;
        }
    }
}