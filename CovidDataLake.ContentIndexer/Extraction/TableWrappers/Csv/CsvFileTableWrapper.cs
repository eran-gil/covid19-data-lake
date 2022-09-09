using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CovidDataLake.ContentIndexer.Extraction.Models;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Csv
{
    class CsvFileTableWrapper : IFileTableWrapper
    {
        private readonly string _originFilename;

        public CsvFileTableWrapper(string filename, string originFilename)
        {
            Filename = filename;
            _originFilename = originFilename;
        }
        public string Filename { get; set; }
        public IEnumerable<KeyValuePair<string, IEnumerable<RawEntry>>> GetColumns()
        {
            Dictionary<string, int> columnLocations;
            try
            {
                using var fileStream = File.OpenRead(Filename);
                var reader = CreateCsvReader(fileStream);
                var columnNames = reader.ReadHeaders();
                columnLocations = Enumerable.Range(0, columnNames.Count)
                    .ToDictionary(
                        columnIndex => columnNames[columnIndex],
                        columnIndex => columnIndex
                    );
            }
            catch (Exception)
            {
                return GetDefaultValue();
            }
            var columnValues = columnLocations.ToDictionary(
                column => column.Key,
                column => GetColumnValues(column.Value)
            );
            return columnValues;
        }

        private IEnumerable<RawEntry> GetColumnValues(int columnLocation)
        {
            using var reader = CreateCsvReader(Filename);
            var columnValues = reader
                .ReadColumn(columnLocation)
                .Where(value => value != null);

            var values = new HashSet<string>();
            foreach (var value in columnValues)
            {
                if (values.Contains(value))
                {
                    continue;
                }

                values.Add(value);
                yield return new RawEntry(_originFilename, value);
            }
        }

        private static CsvFileReader CreateCsvReader(string filename)
        {
            var stream = File.OpenRead(filename);
            stream.Seek(0, SeekOrigin.Begin);
            return CreateCsvReader(stream);
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
