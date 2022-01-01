using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Common;
using CovidDataLake.Common.Probabilistic;
using CovidDataLake.Queries.Models;

namespace CovidDataLake.Queries.Executors
{
    public class CardinalityQueryExecutor : BaseQueryExecutor<CardinalityQuery>
    {
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly string _bucketName;

        public CardinalityQueryExecutor(IAmazonAdapter amazonAdapter, BasicAmazonIndexFileConfiguration indexConfiguration)
        {
            _amazonAdapter = amazonAdapter;
            _bucketName = indexConfiguration.BucketName;
        }

        public override async Task<IEnumerable<QueryResult>> Execute(CardinalityQuery query)
        {
            var filename =
                $"{CommonKeys.METADATA_INDICES_FOLDER_NAME}/{CommonKeys.HLL_FOLDER_NAME}/{query.MetadataKey}.{CommonKeys.HLL_FILE_TYPE}";
            var downloadedFile = await _amazonAdapter.DownloadObjectAsync(_bucketName, filename);
            if (string.IsNullOrEmpty(downloadedFile))
                return Enumerable.Empty<QueryResult>();
            var hll = GetHyperLogLogFromFile(downloadedFile);
            var result = new QueryResult { ["Cardinality"] = hll.Count()};
            return new List<QueryResult> { result };
        }

        public override bool CanHandle(string queryType)
        {
            return queryType == "Cardinality";
        }

        private static HyperLogLog GetHyperLogLogFromFile(string filename)
        {
            using var stream = File.OpenRead(filename);
            return new HyperLogLog(stream);
        }

    }
}
