using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
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
        private string _rootIndexLocalFileName;

        public AmazonRootIndexFileAccess(IRootIndexCache cache, IAmazonAdapter amazonAdapter, ILock lockMechanism, AmazonRootIndexFileConfiguration configuration)
        {
            _cache = cache;
            _amazonAdapter = amazonAdapter;
            _lockMechanism = lockMechanism;
            _lockTimeSpan = TimeSpan.FromSeconds(configuration.LockTimeSpanInSeconds);
            _bucketName = configuration.BucketName;
            _rootIndexName = $"{CommonKeys.INDEX_FOLDER_NAME}/{configuration.RootIndexName}";
            _rootIndexLocalFileName = null;
        }

        public async Task EnterBatch()
        {
            _lockMechanism.TakeLock(CommonKeys.ROOT_INDEX_FILE_LOCK_KEY, _lockTimeSpan);
            _rootIndexLocalFileName = await GetOrCreateRootIndexFile();
        }

        public async Task ExitBatch(bool shouldUpdate = false)
        {
            if (shouldUpdate)
                await _amazonAdapter.UploadObjectAsync(_bucketName, _rootIndexName, _rootIndexLocalFileName);
            _rootIndexLocalFileName = string.Empty;
            _lockMechanism.ReleaseLock(CommonKeys.ROOT_INDEX_FILE_LOCK_KEY);
        }

        public async Task UpdateColumnRanges(SortedSet<RootIndexColumnUpdate> columnMappings)
        {
            //_lockMechanism.ExtendLock(CommonKeys.ROOT_INDEX_FILE_LOCK_KEY, _lockTimeSpan);
            _lockMechanism.TakeLock(CommonKeys.ROOT_INDEX_UPDATE_FILE_LOCK_KEY, _lockTimeSpan);
            using var stream = OptionalFileStream.CreateOptionalFileReadStream(_rootIndexLocalFileName, false);
            var indexRows = GetIndexRowsFromFile(stream);
            var outputRows = MergeIndexWithUpdate(indexRows, columnMappings);
            var outputFileName = Path.Join(CommonKeys.TEMP_FOLDER_NAME, Guid.NewGuid().ToString());
            await WriteIndexRowsToFile(outputFileName, outputRows);
            _rootIndexLocalFileName = outputFileName;
            await _cache.UpdateColumnRanges(columnMappings);
            _lockMechanism.ReleaseLock(CommonKeys.ROOT_INDEX_UPDATE_FILE_LOCK_KEY);
        }

        

        public async Task<string> GetFileNameForColumnAndValue(string column, string val)
        {
            //_lockMechanism.ExtendLock(CommonKeys.ROOT_INDEX_FILE_LOCK_KEY, _lockTimeSpan);
            var cached = await _cache.GetFileNameForColumnAndValue(column, val);
            if (cached != null) return cached;

            using var stream = OptionalFileStream.CreateOptionalFileReadStream(_rootIndexLocalFileName, false);
            var indexRows = GetIndexRowsFromFile(stream);
            var relevantIndexRow =
                indexRows.FirstOrDefault(row => ValidateRowWithRequest(column, val, row));

            if (relevantIndexRow == default(RootIndexRow))
                return CommonKeys.END_OF_INDEX_FLAG;
            var update = new RootIndexColumnUpdate
            {
                ColumnName = column,
                Rows = new SortedSet<RootIndexRow> { relevantIndexRow }
            };

            await _cache.UpdateColumnRanges(new SortedSet<RootIndexColumnUpdate> { update });
            return relevantIndexRow.FileName;
        }

        private async Task<string> GetOrCreateRootIndexFile()
        {
            string downloadedFileName;
            try
            {
                downloadedFileName = await _amazonAdapter.DownloadObjectAsync(_bucketName, _rootIndexName);
            }
            catch (ResourceNotFoundException)
            {
                downloadedFileName = await CreateRootIndexFile();
            }

            return downloadedFileName;
        }

        private async Task<string> CreateRootIndexFile()
        {
            var fileName = $"{CommonKeys.TEMP_FOLDER_NAME}/{Guid.NewGuid()}.txt";
            var file = FileCreator.CreateFileAndPath(fileName);
            file.Close();
            await _amazonAdapter.UploadObjectAsync(_bucketName, _rootIndexName, fileName);
            return fileName;
        }

        private static bool ValidateRowWithRequest(string column, string val, RootIndexRow indexRow)
        {
            return indexRow.ColumnName == column && string.CompareOrdinal(val, indexRow.Max) < 0;
        }

        private static async Task WriteIndexRowsToFile(string outputFileName, IEnumerable<RootIndexRow> outputRows)
        {
            await using var outputFile = FileCreator.OpenFileWriteAndCreatePath(outputFileName);
            await using var outputStreamWriter = new StreamWriter(outputFile);
            foreach (var outputRow in outputRows)
            {
                await outputStreamWriter.WriteObjectToLineAsync(outputRow);
            }
        }

        private static IEnumerable<RootIndexRow> MergeIndexWithUpdate(
            IEnumerable<RootIndexRow> indexRows,
            IEnumerable<RootIndexColumnUpdate> updates)
        {
            var indexRowsEnumerator = indexRows.GetEnumerator();
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
                        currentIndexRow = GetNextIndexRow(indexRowsEnumerator);
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
                        currentIndexRow = GetNextIndexRow(indexRowsEnumerator);
                        continue;
                    }

                    if (string.CompareOrdinal(currentIndexRow.Min, updateRow.Min) < 0)
                    {
                        yield return updateRow;
                    }

                }
            }

            while (currentIndexRow != null)
            {
                yield return currentIndexRow;
                currentIndexRow = GetNextIndexRow(indexRowsEnumerator);
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

        private static RootIndexRow GetNextIndexRow(IEnumerator<RootIndexRow> indexRowsEnumerator)
        {
            var currentIndexRow = indexRowsEnumerator.MoveNext() ? null : indexRowsEnumerator.Current;

            return currentIndexRow;
        }

        private static IEnumerable<RootIndexRow> GetIndexRowsFromFile(OptionalFileStream stream)
        {
            var indexFile = stream.BaseStream;
            var rows = Enumerable.Empty<RootIndexRow>();
            if (indexFile != null)
            {
                rows = indexFile.GetDeserializedRowsFromFileAsync<RootIndexRow>(indexFile.Length);
            }
            return rows;
        }
    }
}
