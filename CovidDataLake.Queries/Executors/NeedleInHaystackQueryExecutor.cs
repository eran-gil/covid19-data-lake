using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Amazon;
using CovidDataLake.Bloom;
using CovidDataLake.Common;
using CovidDataLake.ContentIndexer.Extensions;
using CovidDataLake.ContentIndexer.Indexing;
using CovidDataLake.ContentIndexer.Indexing.Models;
using CovidDataLake.Queries.Models;

namespace CovidDataLake.Queries.Executors
{
    class NeedleInHaystackQueryExecutor : IQueryExecutor<NeedleInHaystackQuery>
    {
        private readonly IRootIndexAccess _rootIndexAccess;
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly string _bucketName;

        public NeedleInHaystackQueryExecutor(IRootIndexAccess rootIndexAccess, IAmazonAdapter amazonAdapter, string bucketName)
        {
            _rootIndexAccess = rootIndexAccess;
            _amazonAdapter = amazonAdapter;
            _bucketName = bucketName;
        }
        public IEnumerable<QueryResult> Execute(NeedleInHaystackQuery query)
        {
            var queryResults = query.Conditions.Select(async condition => await GetFilesMatchingCondition(condition)).ToTaskResults();
            var aggregatedQueryResults = Enumerable.Empty<QueryResult>();
            // ReSharper disable once SwitchStatementHandlesSomeKnownEnumValuesWithDefault
            switch (query.Relation)
            {
                case ConditionRelation.And:
                    aggregatedQueryResults = queryResults.Aggregate(aggregatedQueryResults, IntersectResults);
                    break;
                case ConditionRelation.Or:
                    aggregatedQueryResults = queryResults.Aggregate(aggregatedQueryResults, UnionResults);
                    break;
            }

            return aggregatedQueryResults;
        }

        private static IEnumerable<QueryResult> IntersectResults(IEnumerable<QueryResult> result1,
            IEnumerable<QueryResult> result2)
        {
            return result1.Intersect(result2);
        }

        private static IEnumerable<QueryResult> UnionResults(IEnumerable<QueryResult> result1,
            IEnumerable<QueryResult> result2)
        {
            return result1.Union(result2);
        }

        private async Task<IEnumerable<QueryResult>> GetFilesMatchingCondition(NeedleInHaystackColumnCondition condition)
        {
            //todo: add locks
            //todo: separate to methods
            var defaultResult = Enumerable.Empty<QueryResult>();
            var indexFileName = await _rootIndexAccess.GetFileNameForColumnAndValue(condition.ColumnName, condition.Value);
            if (indexFileName == CommonKeys.END_OF_INDEX_FLAG)
            {
                return defaultResult;
            }

            var downloadedFileName = await _amazonAdapter.DownloadObjectAsync(_bucketName, indexFileName);
            using var indexFile = File.OpenRead(downloadedFileName);
            indexFile.Seek(-(2 * sizeof(long)), SeekOrigin.End);
            var metadataOffset = indexFile.ReadBinaryLongFromStream();
            var bloomOffset = indexFile.ReadBinaryLongFromStream();
            var bloomFilter = await GetBloomFilterFromFile(indexFile, bloomOffset);
            if (!bloomFilter.IsInFilter(condition.Value))
            {
                return defaultResult;
            }
            indexFile.Seek(metadataOffset, SeekOrigin.Begin);
            var metadataRows = indexFile.GetDeserializedRowsFromFileAsync<IndexMetadataSectionModel>(indexFile.Length);
            var relevantSection = default(IndexMetadataSectionModel);
            var endOffset = metadataOffset;
            await foreach (var metadataRow in metadataRows)
            {
                if (relevantSection != default(IndexMetadataSectionModel))
                {
                    endOffset = metadataRow.Offset;
                    break;
                }
                if (string.Compare(metadataRow.Max, condition.Value, StringComparison.Ordinal) < 0 &&
                    string.Compare(metadataRow.Max, condition.Value, StringComparison.Ordinal) > 0)
                {
                    relevantSection = metadataRow;
                }
            }

            if (relevantSection == default(IndexMetadataSectionModel))
            {
                return defaultResult;
            }
            indexFile.Seek(relevantSection.Offset, SeekOrigin.Begin);
            var indexRows = indexFile.GetDeserializedRowsFromFileAsync<IndexValueModel>(endOffset);
            var indexRow = await indexRows.FirstOrDefaultAsync(row => row.Value.Equals(condition.Value));
            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (indexRow == default(IndexValueModel))
            {
                return defaultResult;
            }

            return indexRow.Files.Select(file => new QueryResult(file, new[] {condition.Value}));

        }

        private static async Task<PythonBloomFilter> GetBloomFilterFromFile(Stream stream, long offset)
        {
            if (stream == null)
            {
                return null;
            }

            stream.Seek(offset, SeekOrigin.Begin);
            var bloomOffsetLength = (stream.Length - 2 * sizeof(long)) - offset;
            var serializedBloomFilter = new byte[bloomOffsetLength];
            await stream.ReadAsync(serializedBloomFilter);
            return new PythonBloomFilter(serializedBloomFilter);
        }


    }
}
