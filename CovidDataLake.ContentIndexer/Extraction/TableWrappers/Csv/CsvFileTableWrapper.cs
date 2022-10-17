using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
            _defaultOriginFilenames = new List<StringWrapper> { _originFilename };
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
                var columnCollections = columnsRange.Select(_ => new ColumnWriter()).ToList();
                var produceTask = Task.Run(async () => await WriteColumnsToCollections(columnCollections, lines).ConfigureAwait(false));
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

        private IAsyncEnumerable<RawEntry> GetColumnFromChannel(ColumnWriter rawEntries)
        {
            return rawEntries.ColumnValues
                .NotNull()
                .Distinct()
                .Select(value => new RawEntry(_defaultOriginFilenames, value));
        }

        private static async Task WriteColumnsToCollections(IReadOnlyList<ColumnWriter> columnCollections, IEnumerable<IList<string>> lines)
        {
            foreach (var line in lines)
            {
                await WriteLineToColumnFiles(line, columnCollections).ConfigureAwait(false);
            }

            foreach (var columnCollection in columnCollections)
            {
                columnCollection.FinishColumn();
            }
        }

        private static async Task WriteLineToColumnFiles(IList<string> line, IReadOnlyList<ColumnWriter> columnCollections)
        {
            for (int i = 0; i < line.Count; i++)
            {
                if (string.IsNullOrEmpty(line[i]))
                {
                    continue;
                }

                var column = line[i];
                await columnCollections[i].WriteValue(column).ConfigureAwait(false);
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
