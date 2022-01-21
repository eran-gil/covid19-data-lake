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
    public class FrequencyQueryExecutor : BaseQueryExecutor<FrequencyQuery>
    {

        public FrequencyQueryExecutor(IAmazonAdapter amazonAdapter, BasicAmazonIndexFileConfiguration indexConfiguration) : base(amazonAdapter, indexConfiguration)
        {
        }

        public override async Task<IEnumerable<QueryResult>> Execute(FrequencyQuery query)
        {
            if (string.IsNullOrEmpty(query.MetadataKey) || string.IsNullOrEmpty(query.MetadataValue))
            {
                throw new InvalidQueryFormatException();
            }
            var filename =
                $"{CommonKeys.METADATA_INDICES_FOLDER_NAME}/{CommonKeys.CMS_FOLDER_NAME}/{query.MetadataKey}.{CommonKeys.CMS_FILE_TYPE}";
            var downloadedFile = await this.DownloadIndexFile(filename);
            if (string.IsNullOrEmpty(downloadedFile))
            {
                return Enumerable.Empty<QueryResult>();
            }
            using var stream = File.OpenRead(downloadedFile);
            var cms = new StringCountMinSketch(stream);
            var result = new QueryResult { ["Frequency"] = cms.GetOccurrences(query.MetadataValue) };
            return new List<QueryResult> { result };
        }

        public override bool CanHandle(string queryType)
        {
            return queryType == "Frequency";
        }

    }
}
