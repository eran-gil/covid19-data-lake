﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Common;
using CovidDataLake.Common.Probabilistic;
using CovidDataLake.ContentIndexer.Extensions;
using CovidDataLake.ContentIndexer.Indexing;
using CovidDataLake.ContentIndexer.Indexing.Models;
using CovidDataLake.Queries.Exceptions;
using CovidDataLake.Queries.Models;

namespace CovidDataLake.Queries.Executors
{
    public class NeedleInHaystackQueryExecutor : BaseQueryExecutor<NeedleInHaystackQuery>
    {
        private readonly IRootIndexAccess _rootIndexAccess;

        public NeedleInHaystackQueryExecutor(IRootIndexAccess rootIndexAccess, IAmazonAdapter amazonAdapter, BasicAmazonIndexFileConfiguration indexConfiguration) :
            base(amazonAdapter, indexConfiguration)
        {
            _rootIndexAccess = rootIndexAccess;

        }

        public override bool CanHandle(string queryType)
        {
            return queryType == "NeedleInHaystack";
        }

        public override Task<IEnumerable<QueryResult>> Execute(NeedleInHaystackQuery query)
        {
            if (query.Conditions == null || !query.Conditions.Any())
            {
                throw new InvalidQueryFormatException();
            }
            var queryResults = query.Conditions.Select(async condition => await GetFilesMatchingCondition(condition)).ToTaskResults().SelectMany(r => r);
            var filteredResults = Enumerable.Empty<IGrouping<string, QueryResult>>();
            var groupedResults = queryResults.GroupBy(result => result["FileName"].ToString());
            filteredResults = query.Relation switch
            {
                ConditionRelation.And => groupedResults.Where(group => group.Count() == query.Conditions.Count()),
                ConditionRelation.Or => groupedResults,
                _ => filteredResults
            };

            var mergedResults =
                filteredResults.Select(CreateQueryResultFromGroup);

            return Task.FromResult(mergedResults);
        }

        private static QueryResult CreateQueryResultFromGroup(IGrouping<string, QueryResult> @group)
        {
            var result = new QueryResult
            {
                ["FileName"] = @group.Key,
                ["HitValues"] = @group.SelectMany(g => (IEnumerable<string>)g["HitValues"])
            };
            return result;
        }

        private async Task<IEnumerable<QueryResult>> GetFilesMatchingCondition(NeedleInHaystackColumnCondition condition)
        {
            var defaultResult = Enumerable.Empty<QueryResult>();
            var indexFileName = await _rootIndexAccess.GetFileNameForColumnAndValue(condition.ColumnName, condition.Value);
            if (indexFileName == CommonKeys.END_OF_INDEX_FLAG)
            {
                return defaultResult;
            }

            var downloadedFileName = await this.DownloadIndexFile(indexFileName);//await _amazonAdapter.DownloadObjectAsync(_bucketName, indexFileName);
            if (string.IsNullOrEmpty(downloadedFileName))
            {
                return Enumerable.Empty<QueryResult>();
            }
            using var indexFile = File.OpenRead(downloadedFileName);
            indexFile.Seek(-(2 * sizeof(long)), SeekOrigin.End);
            var metadataOffset = indexFile.ReadBinaryLongFromStream();
            if (!await VerifyConditionWithBloomFilter(condition, indexFile))
            {
                return defaultResult;
            }

            var (relevantSection, endOffset) = await GetRelevantSectionInIndex(indexFile, condition, metadataOffset);
            if (relevantSection == default(IndexMetadataSectionModel))
            {
                return defaultResult;
            }
            var indexRow = await GetIndexRowForCondition(condition, indexFile, relevantSection, endOffset);
            return indexRow == default(IndexValueModel)
                ? defaultResult
                : indexRow.Files.Select(file =>
                    new QueryResult { ["FileName"] = file, ["HitValues"] = new[] { condition.Value } });
        }

        private static async Task<IndexValueModel> GetIndexRowForCondition(NeedleInHaystackColumnCondition condition, FileStream indexFile,
            IndexMetadataSectionModel relevantSection, long endOffset)
        {
            indexFile.Seek(relevantSection.Offset, SeekOrigin.Begin);
            var indexRows = indexFile.GetDeserializedRowsFromFileAsync<IndexValueModel>(endOffset);
            var indexRow = await indexRows.FirstOrDefaultAsync(row => row.Value.Equals(condition.Value));
            return indexRow;
        }

        private static async Task<bool> VerifyConditionWithBloomFilter(NeedleInHaystackColumnCondition condition,
            Stream indexFile)
        {
            var bloomOffset = indexFile.ReadBinaryLongFromStream();
            var bloomFilter = await GetBloomFilterFromFile(indexFile, bloomOffset);
            return bloomFilter.IsInFilter(condition.Value);
        }

        private static async Task<Tuple<IndexMetadataSectionModel, long>> GetRelevantSectionInIndex(FileStream indexFile, NeedleInHaystackColumnCondition condition,
            long metadataOffset)
        {
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

            return new Tuple<IndexMetadataSectionModel, long>(relevantSection, endOffset);
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
