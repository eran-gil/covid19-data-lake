using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.Amazon;
using CovidDataLake.Common;
using CovidDataLake.Common.Locking;
using CovidDataLake.ContentIndexer.Extensions;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public class AmazonRootIndexFileAccess : IRootIndexFileAccess
    {
        private readonly IRootIndexCache _cache;
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly ILock _lockMechanism;
        private readonly string _bucketName;
        private readonly string _rootIndexName;
        private readonly TimeSpan _lockTimeSpan = TimeSpan.FromSeconds(30); //todo: get from config

        public AmazonRootIndexFileAccess(IRootIndexCache cache, IAmazonAdapter amazonAdapter, ILock lockMechanism)
        {
            _cache = cache;
            _amazonAdapter = amazonAdapter;
            _lockMechanism = lockMechanism;
            _bucketName = ""; //todo: initialize from config
            _rootIndexName = ""; //todo: initialize from config
        }

        public async Task UpdateColumnRanges(SortedSet<RootIndexColumnUpdate> columnMappings)
        {
            await _lockMechanism.TakeLockAsync(CommonKeys.RootIndexFileLockKey, _lockTimeSpan);
            var downloadedFileName = await _amazonAdapter.DownloadObjectAsync(_bucketName, _rootIndexName);
            var indexRows = GetIndexRowsFromFile(downloadedFileName);
            var outputRows = MergeIndexWithUpdate(indexRows, columnMappings);
            var outputFileName = downloadedFileName + "_new";
            await WriteIndexRowsToFile(outputFileName, outputRows);
            await _amazonAdapter.UploadObjectAsync(_bucketName, _rootIndexName, outputFileName);
            await _lockMechanism.ReleaseLockAsync(CommonKeys.RootIndexFileLockKey);
            await _cache.UpdateColumnRanges(columnMappings);
        }

        public async Task<string> GetFileNameForColumnAndValue(string column, ulong val)
        {
            var cached = await _cache.GetFileNameForColumnAndValue(column, val);
            if (cached != null) return cached;

            await _lockMechanism.TakeLockAsync(CommonKeys.RootIndexFileLockKey, _lockTimeSpan);
            var downloadedFileName = await _amazonAdapter.DownloadObjectAsync(_bucketName, _rootIndexName);
            var indexRows = GetIndexRowsFromFile(downloadedFileName);
            var relevantIndexRow =
                await indexRows.Where(row => ValidateRowWithRequest(column, val, row)).FirstOrDefaultAsync();
            if (relevantIndexRow == null) return null;
            await _lockMechanism.ReleaseLockAsync(CommonKeys.RootIndexFileLockKey);

            var update = new RootIndexColumnUpdate
                {ColumnName = column, Rows = new List<RootIndexRow> {relevantIndexRow}};
            await _cache.UpdateColumnRanges(new SortedSet<RootIndexColumnUpdate>{update});

            return relevantIndexRow.FileName;
        }

        private static bool ValidateRowWithRequest(string column, ulong val, RootIndexRow indexRow)
        {
            return indexRow.ColumnName == column && indexRow.Min <= val && indexRow.Max >= val;
        }

        private static async Task WriteIndexRowsToFile(string outputFileName, IAsyncEnumerable<RootIndexRow> outputRows)
        {
            using var outputFile = File.OpenWrite(outputFileName);
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

                    while (currentIndexRow != null && 
                           (string.Compare(currentIndexRow.ColumnName, updateRow.ColumnName, StringComparison.InvariantCulture) > 0
                            || currentIndexRow.FileName != updateRow.FileName
                            && currentIndexRow.Min < updateRow.Min))
                    {
                        yield return currentIndexRow;
                        currentIndexRow = await GetNextIndexRow(indexRowsEnumerator);
                    }

                    if (currentIndexRow == null)
                    {
                        continue;
                    }

                    if (string.Compare(currentIndexRow.ColumnName, updateRow.ColumnName,
                        StringComparison.InvariantCulture) < 0)
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

                    if (currentIndexRow.Min > updateRow.Min)
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

        private static IAsyncEnumerable<RootIndexRow> GetIndexRowsFromFile(string filename)
        {
            if (new FileInfo(filename).Length == 0)
            {
                return AsyncEnumerable.Empty<RootIndexRow>();
            }

            using var indexFile = File.OpenRead(filename);
            var rows = indexFile.GetDeserializedRowsFromFileAsync<RootIndexRow>(indexFile.Length);
            return rows;
        }
    }
}