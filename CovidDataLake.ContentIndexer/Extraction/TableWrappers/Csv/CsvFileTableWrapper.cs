using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
using CovidDataLake.Common;
using CovidDataLake.ContentIndexer.Extraction.Models;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Csv
{
    class CsvFileTableWrapper : IFileTableWrapper
    {
        // ReSharper disable once PrivateFieldCanBeConvertedToLocalVariable
        private readonly StringWrapper _originFilename;
        private readonly List<StringWrapper> _defaultOriginFilenames;

        public CsvFileTableWrapper(string filename, string originFilename)
        {
            Filename = filename;
            _originFilename = new StringWrapper(originFilename);
            _defaultOriginFilenames = new List<StringWrapper>{_originFilename};
        }
        public string Filename { get; set; }
        public IEnumerable<KeyValuePair<string, IAsyncEnumerable<RawEntry>>> GetColumns()
        {
            try
            {
                var fileStream = File.OpenRead(Filename);
                var reader = CreateCsvReader(fileStream);
                var columnNames = reader.ReadHeaders();
                var lines = reader.ReadLines();
                var columnsRange = Enumerable.Range(0, columnNames.Count).ToList();
                var columnLocations = columnsRange.ToDictionary(
                    columnIndex => columnNames[columnIndex],
                    columnIndex => columnIndex
                );
                var columnCollections = columnsRange.Select(_ => Channel.CreateUnbounded<string>()).ToList();
                var produceTask = Task.Run(() => WriteColumnsToCollections(columnCollections, lines));
                produceTask.ContinueWith(_ =>
                {
                    reader.Dispose();
                    fileStream.Close();
                });
                var columnValues = columnLocations.ToDictionary(
                    column => column.Key,
                    column => GetColumnFromChannel(columnCollections[column.Value]));
                return columnValues;
            }
            catch (Exception)
            {
                return GetDefaultValue();
            }
            
        }

        private IAsyncEnumerable<RawEntry> GetColumnFromChannel(Channel<string> rawEntries)
        {
            return rawEntries.Reader
                .ReadAllAsync()
                .NotNull()
                .Distinct()
                .Select(value => new RawEntry(_defaultOriginFilenames, value));
        }

        private static async Task WriteColumnsToCollections(IReadOnlyList<Channel<string>> columnCollections, IEnumerable<IList<string>> lines)
        {
            foreach (var line in lines)
            {
                await WriteLineToColumnFiles(line, columnCollections);
            }

            foreach (var columnCollection in columnCollections)
            {
                columnCollection.Writer.Complete();
            }
        }

        private static async Task WriteLineToColumnFiles(IList<string> line, IReadOnlyList<Channel<string>> columnCollections)
        {
            for (int i = 0; i < line.Count; i++)
            {
                if (string.IsNullOrEmpty(line[i]))
                {
                    continue;
                }

                var column = line[i];
                await columnCollections[i].Writer.WriteAsync(column);
            }
        }

        private static CsvFileReader CreateCsvReader(Stream stream)
        {
            var csvReader = new CsvFileReader(stream);
            return csvReader;
        }

        private static IEnumerable<KeyValuePair<string, IAsyncEnumerable<RawEntry>>> GetDefaultValue()
        {
            return Enumerable.Empty<KeyValuePair<string, IAsyncEnumerable<RawEntry>>>();
        }

    }
}
