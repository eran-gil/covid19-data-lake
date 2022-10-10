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

namespace CovidDataLake.ContentIndexer.Indexing.NeedleInHaystack.RootIndex
{
    public class AmazonRootIndexAccess : IRootIndexFileAccess
    {
        private readonly IRootIndexCache _cache;
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly ILock _lockMechanism;
        private readonly string _bucketName;
        private readonly string _rootIndexName;
        private readonly TimeSpan _lockTimeSpan;
        private string _rootIndexLocalFileName;
        private bool _isCacheLoaded;

        public AmazonRootIndexAccess(IRootIndexCache cache, IAmazonAdapter amazonAdapter, ILock lockMechanism, AmazonRootIndexFileConfiguration configuration)
        {
            _cache = cache;
            _amazonAdapter = amazonAdapter;
            _lockMechanism = lockMechanism;
            _lockTimeSpan = TimeSpan.FromSeconds(configuration.LockTimeSpanInSeconds);
            _bucketName = configuration.BucketName;
            _rootIndexName = $"{CommonKeys.INDEX_FOLDER_NAME}/{CommonKeys.COLUMN_INDICES_FOLDER_NAME}/{configuration.RootIndexName}";
            _rootIndexLocalFileName = null;
            _isCacheLoaded = false;
        }

        public async Task EnterBatch()
        {
            await _lockMechanism.TakeLock(CommonKeys.ROOT_INDEX_FILE_LOCK_KEY, _lockTimeSpan);
            _rootIndexLocalFileName = await GetOrCreateRootIndexFile();
            await LoadIndexToCache();
            _isCacheLoaded = true;
        }

        public async Task ExitBatch(bool shouldUpdate = false)
        {
            if (shouldUpdate)
                await _amazonAdapter.UploadObjectAsync(_bucketName, _rootIndexName, _rootIndexLocalFileName);
            _rootIndexLocalFileName = string.Empty;
            _isCacheLoaded = false;
            await _lockMechanism.ReleaseLock(CommonKeys.ROOT_INDEX_FILE_LOCK_KEY);
        }

        public async Task UpdateColumnRanges(IReadOnlyCollection<RootIndexColumnUpdate> columnMappings)
        {
            await _lockMechanism.TakeLock(CommonKeys.ROOT_INDEX_UPDATE_FILE_LOCK_KEY, _lockTimeSpan);
            using var stream = OptionalFileStream.CreateOptionalFileReadStream(_rootIndexLocalFileName);
            var indexRows = GetIndexRowsFromFile(stream);
            var outputRows = MergeIndexWithUpdate(indexRows, columnMappings);
            var outputFileName = Path.Join(CommonKeys.TEMP_FOLDER_NAME, Guid.NewGuid().ToString());
            await WriteIndexRowsToFile(outputFileName, outputRows);
            _rootIndexLocalFileName = outputFileName;
            await _lockMechanism.ReleaseLock(CommonKeys.ROOT_INDEX_UPDATE_FILE_LOCK_KEY);
            await _cache.UpdateColumnRanges(columnMappings);
        }


        public async Task<string> GetFileNameForColumnAndValue(string column, string val)
        {
            var cached = await _cache.GetFileNameForColumnAndValue(column, val);
            if (cached != null) return cached;
            if (_isCacheLoaded) return CommonKeys.END_OF_INDEX_FLAG;
            using var stream = OptionalFileStream.CreateOptionalFileReadStream(_rootIndexLocalFileName);
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

        private async Task LoadIndexToCache()
        {
            using var stream = OptionalFileStream.CreateOptionalFileReadStream(_rootIndexLocalFileName);
            var indexRows = GetIndexRowsFromFile(stream);
            await _cache.LoadAllEntries(indexRows);
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
            var indexColumns = MapIndexToDictionary(indexRows);
            var updateIndexColumns = MapUpdatesToDictionary(updates);
            MergeUpdateToIndex(updateIndexColumns, indexColumns);
            var newIndexEntries = indexColumns.SelectMany(SortFilesInColumn);
            return newIndexEntries;
        }

        private static void MergeUpdateToIndex(Dictionary<string, Dictionary<string, List<RootIndexRow>>> updateIndexColumns, Dictionary<string, Dictionary<string, List<RootIndexRow>>> indexColumns)
        {
            foreach (var (columnName, fileDictionary) in updateIndexColumns)
            {
                if (indexColumns.ContainsKey(columnName))
                {
                    var indexColumn = indexColumns[columnName];
                    MergeColumnUpdateToIndexColumn(fileDictionary, indexColumn);
                }
                else
                {
                    indexColumns[columnName] = fileDictionary;
                }
            }
        }

        private static Dictionary<string, Dictionary<string, List<RootIndexRow>>> MapUpdatesToDictionary(IEnumerable<RootIndexColumnUpdate> updates)
        {
            return updates
                .ToDictionary(update => update.ColumnName, update => ToDictionaryByFileName(update.Rows));
        }

        private static Dictionary<string, Dictionary<string, List<RootIndexRow>>> MapIndexToDictionary(IEnumerable<RootIndexRow> indexRows)
        {
            return indexRows
                .GroupBy(row => row.ColumnName)
                .ToDictionary(column => column.Key, ToDictionaryByFileName);
        }

        private static IOrderedEnumerable<RootIndexRow> SortFilesInColumn(KeyValuePair<string, Dictionary<string, List<RootIndexRow>>> indexColumn)
        {
            return indexColumn
                .Value
                .SelectMany(fileEntries => fileEntries.Value)
                .OrderBy(row => row.Min);
        }

        private static void MergeColumnUpdateToIndexColumn(Dictionary<string, List<RootIndexRow>> fileDictionary, IDictionary<string, List<RootIndexRow>> indexColumn)
        {
            foreach (var (fileName, entries) in fileDictionary)
            {
                indexColumn[fileName] = entries;
            }
        }

        private static Dictionary<string, List<RootIndexRow>> ToDictionaryByFileName(IEnumerable<RootIndexRow> column)
        {
            return column
                .GroupBy(row => row.FileName)
                .ToDictionary(
                    file => file.Key,
                    file => file.ToList()
                );
        }

        private static IEnumerable<RootIndexRow> GetIndexRowsFromFile(OptionalFileStream stream)
        {
            var indexFile = stream.BaseStream;
            var rows = Enumerable.Empty<RootIndexRow>();
            if (indexFile != null)
            {
                rows = indexFile.GetDeserializedRowsFromFile<RootIndexRow>(0, indexFile.Length);
            }
            return rows;
        }
    }
}
