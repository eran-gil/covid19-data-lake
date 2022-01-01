using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Common;
using CovidDataLake.Common.Probabilistic;
using CovidDataLake.Queries.Models;

namespace CovidDataLake.Queries.Executors
{
    public class FrequencyQueryExecutor : BaseQueryExecutor<FrequencyQuery>
    {
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly string _bucketName;

        public FrequencyQueryExecutor(IAmazonAdapter amazonAdapter, BasicAmazonIndexFileConfiguration indexConfiguration)
        {
            _amazonAdapter = amazonAdapter;
            _bucketName = indexConfiguration.BucketName;
        }

        public override async Task<IEnumerable<QueryResult>> Execute(FrequencyQuery query)
        {
            var filename =
                $"{CommonKeys.METADATA_INDICES_FOLDER_NAME}/{CommonKeys.CMS_FOLDER_NAME}/{query.MetadataKey}.{CommonKeys.CMS_FILE_TYPE}";
            var downloadedFile = await _amazonAdapter.DownloadObjectAsync(_bucketName, filename);
            if (string.IsNullOrEmpty(downloadedFile))
                return Enumerable.Empty<QueryResult>();
            var cms = new PythonCountMinSketch(downloadedFile);
            var result = new QueryResult { ["Frequency"] = cms.GetOccurrences(query.MetadataValue) };
            return new List<QueryResult> { result };
        }

        public override bool CanHandle(string queryType)
        {
            return queryType == "Frequency";
        }

    }
}
