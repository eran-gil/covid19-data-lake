using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Extraction.Models;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Csv
{
    class CsvFileTableWrapper : IFileTableWrapper
    {
        // ReSharper disable once PrivateFieldCanBeConvertedToLocalVariable
        private readonly StringWrapper _originFilename;
        private readonly ImmutableHashSet<StringWrapper> _defaultOriginFileNames;

        public CsvFileTableWrapper(string filename, string originFilename)
        {
            Filename = filename;
            _originFilename = new StringWrapper(originFilename);
            _defaultOriginFileNames = ImmutableHashSet.Create(_originFilename);
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
                var columnCollections = columnsRange.Select(_ => new ColumnChannelWriter()).ToList();
                var produceTask = Task.Run(async () => await WriteColumnsToCollections(columnCollections, lines).ConfigureAwait(false));
                produceTask.ContinueWith(_ =>
                {
                    reader.Dispose();
                    fileStream.Close();
                });
                var columnValues = columnLocations.ToDictionary(
                        column => column.Key,
                        column => columnCollections[column.Value].GetColumnEntries(_defaultOriginFileNames));
                return columnValues;
            }
            catch (Exception)
            {
                return GetDefaultValue();
            }

        }

        private static async Task WriteColumnsToCollections(IReadOnlyList<IColumnWriter> columnCollections, IEnumerable<IList<string>> lines)
        {
            foreach (var line in lines)
            {
                await WriteLineToColumnWriters(line, columnCollections).ConfigureAwait(false);
            }

            foreach (var columnCollection in columnCollections)
            {
                columnCollection.FinishWriting();
            }
        }

        private static async Task WriteLineToColumnWriters(IList<string> line, IReadOnlyList<IColumnWriter> columnCollections)
        {
            for (int i = 0; i < line.Count; i++)
            {
                if (string.IsNullOrEmpty(line[i]))
                {
                    continue;
                }

                var column = line[i];
                await columnCollections[i].WriteValueAsync(column).ConfigureAwait(false);
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
