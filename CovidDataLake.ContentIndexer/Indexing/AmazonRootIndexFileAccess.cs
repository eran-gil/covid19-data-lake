using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Amazon.S3;
using CovidDataLake.Amazon;
using CovidDataLake.Common;
using CovidDataLake.Common.Files;
using CovidDataLake.Common.Locking;
using CovidDataLake.ContentIndexer.Configuration;
using CovidDataLake.ContentIndexer.Extensions;
using CovidDataLake.ContentIndexer.Indexing.Models;
using CovidDataLake.Storage.Utils;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public class AmazonRootIndexFileAccess : IRootIndexFileAccess
    {
        private readonly IRootIndexCache _cache;
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly ILock _lockMechanism;
        private readonly string _bucketName;
        private readonly string _rootIndexName;
        private readonly TimeSpan _lockTimeSpan;

        public AmazonRootIndexFileAccess(IRootIndexCache cache, IAmazonAdapter amazonAdapter, ILock lockMechanism, AmazonRootIndexFileConfiguration configuration)
        {
            _cache = cache;
            _amazonAdapter = amazonAdapter;
            _lockMechanism = lockMechanism;
            _lockTimeSpan = TimeSpan.FromSeconds(configuration.LockTimeSpanInSeconds);
            _bucketName = configuration.BucketName;
            _rootIndexName = $"{CommonKeys.INDEX_FOLDER_NAME}/{configuration.RootIndexName}";
        }

        public async Task UpdateColumnRanges(SortedSet<RootIndexColumnUpdate> columnMappings)
        {
            await _lockMechanism.TakeLockAsync(CommonKeys.ROOT_INDEX_FILE_LOCK_KEY, _lockTimeSpan);
            var downloadedFileName = await _amazonAdapter.DownloadObjectAsync(_bucketName, _rootIndexName);
            using var stream = OptionalFileStream.CreateOptionalFileReadStream(downloadedFileName);
            var indexRows = GetIndexRowsFromFile(stream);
            var outputRows = MergeIndexWithUpdate(indexRows, columnMappings);
            var outputFileName = downloadedFileName + "_new";
            await WriteIndexRowsToFile(outputFileName, outputRows);
            await _amazonAdapter.UploadObjectAsync(_bucketName, _rootIndexName, outputFileName);
            File.Delete(downloadedFileName);
            await _lockMechanism.ReleaseLockAsync(CommonKeys.ROOT_INDEX_FILE_LOCK_KEY);
            await _cache.UpdateColumnRanges(columnMappings);
        }

        public async Task<string> GetFileNameForColumnAndValue(string column, string val)
        {
            var cached = await _cache.GetFileNameForColumnAndValue(column, val);
            if (cached != null) return cached;

            await _lockMechanism.TakeLockAsync(CommonKeys.ROOT_INDEX_FILE_LOCK_KEY, _lockTimeSpan);
            var downloadedFileName = "__NOT_EXISTING__";
            try
            {
                downloadedFileName = await _amazonAdapter.DownloadObjectAsync(_bucketName, _rootIndexName);
                
            }
            catch (AmazonS3Exception e)
            {
                if (e.StatusCode == HttpStatusCode.NotFound)
                {
                    await CreateRootIndexFile();
                }
            }
            await _lockMechanism.ReleaseLockAsync(CommonKeys.ROOT_INDEX_FILE_LOCK_KEY);

            using var stream = OptionalFileStream.CreateOptionalFileReadStream(downloadedFileName);
            var indexRows = GetIndexRowsFromFile(stream);
            var relevantIndexRow =
                await indexRows.Where(row => ValidateRowWithRequest(column, val, row)).FirstOrDefaultAsync();
            
            if (relevantIndexRow == default(RootIndexRow))
                return CommonKeys.END_OF_INDEX_FLAG;
            var update = new RootIndexColumnUpdate
            {
                ColumnName = column, Rows = new SortedSet<RootIndexRow> {relevantIndexRow}
            };

            await _cache.UpdateColumnRanges(new SortedSet<RootIndexColumnUpdate>{update});
            return relevantIndexRow.FileName;
        }

        private async Task CreateRootIndexFile()
        {
            var fileName = $"{CommonKeys.TEMP_FOLDER_NAME}/{Guid.NewGuid()}.txt";
            var file = FileCreator.CreateFileAndPath(fileName);
            file.Close();
            await _amazonAdapter.UploadObjectAsync(_bucketName, _rootIndexName, fileName);
            File.Delete(fileName);
        }

        private static bool ValidateRowWithRequest(string column, string val, RootIndexRow indexRow)
        {
            return indexRow.ColumnName == column && string.CompareOrdinal(val, indexRow.Max) < 0;
        }

        private static async Task WriteIndexRowsToFile(string outputFileName, IAsyncEnumerable<RootIndexRow> outputRows)
        {
            using var outputFile = FileCreator.OpenFileWriteAndCreatePath(outputFileName);
            using var outputStreamWriter = new StreamWriter(outputFile);
            await foreach (var outputRow in outputRows)
            {
                await outputStreamWriter.WriteObjectToLineAsync(outputRow);
            }
        }

        private static async IAsyncEnumerable<RootIndexRow> MergeIndexWithUpdate(
            IAsyncEnumerable<RootIndexRow> indexRows,
            IEnumerable<RootIndexColumnUpdate> updates)
        {
            var indexRowsEnumerator = indexRows.GetAsyncEnumerator();
            var currentIndexRow = indexRowsEnumerator.Current;
            foreach (var currentUpdate in updates)
            {
                foreach (var updateRow in currentUpdate.Rows)
                {
                    if (currentIndexRow == null)
                    {
                        yield return updateRow;
                        continue;
                    }

                    while (ShouldWriteOriginalIndexBeforeUpdate(currentIndexRow, updateRow))
                    {
                        yield return currentIndexRow;
                        currentIndexRow = await GetNextIndexRow(indexRowsEnumerator);
                    }

                    if (currentIndexRow == null)
                    {
                        continue;
                    }

                    if (ShouldWriteUpdateBeforeOriginalIndex(currentIndexRow, updateRow))
                    {
                        yield return updateRow;
                        continue;
                    }

                    if (updateRow.FileName == currentIndexRow.FileName)
                    {
                        currentIndexRow.Min = updateRow.Min;
                        currentIndexRow.Max = updateRow.Max;
                        yield return currentIndexRow;
                        currentIndexRow = await GetNextIndexRow(indexRowsEnumerator);
                        continue;
                    }

                    if (string.CompareOrdinal(currentIndexRow.Min , updateRow.Min) < 0)
                    {
                        yield return updateRow;
                    }

                }
            }

            while (currentIndexRow != null)
            {
                yield return currentIndexRow;
                currentIndexRow = await GetNextIndexRow(indexRowsEnumerator);
            }
        }

        private static bool ShouldWriteUpdateBeforeOriginalIndex(RootIndexRow currentIndexRow, RootIndexRow updateRow)
        {
            return string.Compare(currentIndexRow.ColumnName, updateRow.ColumnName,
                StringComparison.InvariantCulture) < 0;
        }

        private static bool ShouldWriteOriginalIndexBeforeUpdate(RootIndexRow currentIndexRow, RootIndexRow updateRow)
        {
            return currentIndexRow != null && 
                   (string.Compare(currentIndexRow.ColumnName, updateRow.ColumnName, StringComparison.InvariantCulture) > 0
                    || (currentIndexRow.FileName != updateRow.FileName
                        && string.CompareOrdinal(currentIndexRow.Min, updateRow.Min) < 0));
        }

        private static async Task<RootIndexRow> GetNextIndexRow(IAsyncEnumerator<RootIndexRow> indexRowsEnumerator)
        {
            RootIndexRow currentIndexRow;
            if (await indexRowsEnumerator.MoveNextAsync())
            {
                currentIndexRow = null;
            }
            else
            {
                currentIndexRow = indexRowsEnumerator.Current;
            }

            return currentIndexRow;
        }

        private static IAsyncEnumerable<RootIndexRow> GetIndexRowsFromFile(OptionalFileStream stream)
        {
            var indexFile = stream.BaseStream;
            var rows = AsyncEnumerable.Empty<RootIndexRow>();
            if (indexFile != null)
            {
                rows = indexFile.GetDeserializedRowsFromFileAsync<RootIndexRow>(indexFile.Length);
            }
            return rows;
        }
    }
}
