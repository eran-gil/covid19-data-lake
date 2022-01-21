using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Common;
using CovidDataLake.Common.Probabilistic;
using CovidDataLake.Queries.Exceptions;
using CovidDataLake.Queries.Models;

namespace CovidDataLake.Queries.Executors
{
    public class CardinalityQueryExecutor : BaseQueryExecutor<CardinalityQuery>
    {

        public CardinalityQueryExecutor(IAmazonAdapter amazonAdapter, BasicAmazonIndexFileConfiguration indexConfiguration) : base(amazonAdapter, indexConfiguration)
        {
        }

        public override async Task<IEnumerable<QueryResult>> Execute(CardinalityQuery query)
        {
            if (string.IsNullOrEmpty(query.MetadataKey))
            {
                throw new InvalidQueryFormatException();
            }
            var filename =
                $"{CommonKeys.METADATA_INDICES_FOLDER_NAME}/{CommonKeys.HLL_FOLDER_NAME}/{query.MetadataKey}.{CommonKeys.HLL_FILE_TYPE}";
            var downloadedFile = await this.DownloadIndexFile(filename);
            if (string.IsNullOrEmpty(downloadedFile))
            {
                return Enumerable.Empty<QueryResult>();
            }

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
