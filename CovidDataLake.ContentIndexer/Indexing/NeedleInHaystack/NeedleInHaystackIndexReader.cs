using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Common;
using CovidDataLake.Common.Files;
using CovidDataLake.ContentIndexer.Extensions;
using CovidDataLake.ContentIndexer.Extraction.Models;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing.NeedleInHaystack
{
    public class NeedleInHaystackIndexReader
    {
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly string _bucketName;

        public NeedleInHaystackIndexReader(IAmazonAdapter amazonAdapter, BasicAmazonIndexFileConfiguration indexConfig)
        {
            _amazonAdapter = amazonAdapter;
            _bucketName = indexConfig.BucketName!;
        }

        public async Task<KeyValuePair<string, string>> DownloadIndexFile(string columnName, string indexFilename)
        {
            var downloadedFilename =
                NeedleInHaystackUtils.CreateNewColumnIndexFileName(columnName);
            if (indexFilename != CommonKeys.END_OF_INDEX_FLAG)
            {
                downloadedFilename = await _amazonAdapter.DownloadObjectAsync(_bucketName, indexFilename).ConfigureAwait(false);
            }
            else
            {
                indexFilename = downloadedFilename;
            }

            return new KeyValuePair<string, string>(indexFilename, downloadedFilename);
        }

        public static IDictionary<string, ImmutableHashSet<StringWrapper>> GetIndexFromFile(string indexLocalFilename)
        {
            var originalIndexValues = Enumerable.Empty<IndexValueModel>();

            using var fileStream = OptionalFileStream.CreateOptionalFileReadStream(indexLocalFilename);
            if (fileStream.BaseStream != null)
            {
                originalIndexValues = GetIndexValuesFromFile(fileStream.BaseStream);
            }
            var indexDictionary = new Dictionary<string, ImmutableHashSet<StringWrapper>>();
            foreach (var indexValue in originalIndexValues)
            {
                indexDictionary[indexValue.Value] = indexValue.GetUniqueFiles();
            }

            return indexDictionary;
        }

        private static IEnumerable<IndexValueModel> GetIndexValuesFromFile(FileStream inputFile)
        {
            inputFile.Seek(-(2 * sizeof(long)), SeekOrigin.End);
            var metadataOffset = inputFile.ReadBinaryLongFromStream();
            var rows = inputFile.GetDeserializedRowsFromFile<IndexValueModel>(0, metadataOffset);
            return rows;
        }
    }
}
