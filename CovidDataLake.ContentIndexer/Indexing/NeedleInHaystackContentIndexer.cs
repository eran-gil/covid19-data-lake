﻿using System.Collections.Generic;
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

        public NeedleInHaystackContentIndexer(IStringHash hasher, IIndexFileWriter indexFileWriter, IRootIndexAccess rootIndexAccess)
        {
            _hasher = hasher;
            _indexFileWriter = indexFileWriter;
            _rootIndexAccess = rootIndexAccess;
        }
        public void IndexTable(IFileTableWrapper tableWrapper)
        {
            var columns = tableWrapper.GetColumns();
            var hashedColumns = new Dictionary<string, IList<ulong>>();
            foreach (var columnId in columns.Keys)
            {
                hashedColumns[columnId] = columns[columnId].Select(s => _hasher.HashStringToUlong(s)).ToList();
            }

            var valuesToFilesMapping = hashedColumns.SelectMany(GetFileMappingForColumn);
            
            var fileGroups = valuesToFilesMapping.GroupBy(kvp => kvp.Value);

            var updateIndexTasks = fileGroups.Select(WriteValuesGroupToFile).ToArray();
            Task.WaitAll(updateIndexTasks);
        }

        private async Task WriteValuesGroupToFile(IGrouping<string, KeyValuePair<ulong, string>> fileGroup)
        {
            await _indexFileWriter.WriteValuesToIndexFile(fileGroup.Select(kvp => kvp.Key), fileGroup.Key);
        }

        private IList<KeyValuePair<ulong, string>> GetFileMappingForColumn(KeyValuePair<string, IList<ulong>> column)
        {
            var mapping = column.Value.AsParallel().Select(async val => 
                new KeyValuePair<ulong, string>(val, await _rootIndexAccess.GetFileNameForColumnAndValue(column.Key, val)))
                .Select(t => t.Result).ToList();
            return mapping;
        }
    }

}