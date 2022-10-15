using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CovidDataLake.Common;
using CovidDataLake.ContentIndexer.Extensions;
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
        public IEnumerable<KeyValuePair<string, IEnumerable<RawEntry>>> GetColumns()
        {
            try
            {
                using var fileStream = File.OpenRead(Filename);
                using var reader = CreateCsvReader(fileStream);
                var columnNames = reader.ReadHeaders();
                var lines = reader.ReadLines();
                var columnsRange = Enumerable.Range(0, columnNames.Count).ToList();
                var columnLocations = columnsRange.ToDictionary(
                    columnIndex => columnNames[columnIndex],
                    columnIndex => columnIndex
                );
                var columnWriters = ConvertColumnsToFiles(columnsRange, lines);
                var columnValues = columnLocations.ToDictionary(
                    column => column.Key,
                    column => GetEntriesFromColumnFile(columnWriters, column));
                return columnValues;
            }
            catch (Exception)
            {
                return GetDefaultValue();
            }
            
        }

        private IEnumerable<RawEntry> GetEntriesFromColumnFile(IReadOnlyList<ColumnWriter> columnWriters, KeyValuePair<string, int> column)
        {
            var columnStream = columnWriters[column.Value].BaseStream;
            columnStream.Seek(0, SeekOrigin.Begin);
            columnWriters[column.Value].Dispose();
            return ReadColumnValuesFromStream(columnStream);
        }

        private static List<ColumnWriter> ConvertColumnsToFiles(IEnumerable<int> columnsRange, IEnumerable<IList<string>> lines)
        {
            var columnWriters = columnsRange.Select(_ => CreateColumnStream()).ToList();
            foreach (var line in lines)
            {
                WriteLineToColumnFiles(line, columnWriters);
            }
            return columnWriters;
        }

        private static void WriteLineToColumnFiles(IList<string> line, IReadOnlyList<ColumnWriter> columnWriters)
        {
            for (int i = 0; i < line.Count; i++)
            {
                if (string.IsNullOrEmpty(line[i]))
                {
                    continue;
                }

                var column = line[i];
                columnWriters[i].WriteValue(column);
            }
        }

        private static ColumnWriter CreateColumnStream()
        {
            var fileName = Path.Join(CommonKeys.TEMP_FOLDER_NAME, Guid.NewGuid().ToString());
            var outFile = File.Open(fileName, FileMode.Create, FileAccess.ReadWrite);
            var outStream = new ColumnWriter(outFile);
            return outStream;
        }

        private IEnumerable<RawEntry> ReadColumnValuesFromStream(Stream columnStream)
        {
            using var columnReader = new StreamReader(columnStream);
            var columnValues = columnReader
                .ReadLines()
                .Select(value => new RawEntry(_defaultOriginFilenames, value));
            foreach (var columnValue in columnValues)
            {
                yield return columnValue;
            }
        }   

        private static CsvFileReader CreateCsvReader(Stream stream)
        {
            var csvReader = new CsvFileReader(stream);
            return csvReader;
        }

        private static IEnumerable<KeyValuePair<string, IEnumerable<RawEntry>>> GetDefaultValue()
        {
            return Enumerable.Empty<KeyValuePair<string, IEnumerable<RawEntry>>>();
        }

    }
}
