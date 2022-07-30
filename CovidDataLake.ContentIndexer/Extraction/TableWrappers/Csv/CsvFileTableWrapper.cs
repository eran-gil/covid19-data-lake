using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Extraction.Models;
using CsvHelper;
using CsvHelper.Configuration;
using DataAccess;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Csv
{
    class CsvFileTableWrapper : IFileTableWrapper
    {
        private readonly CsvConfiguration _csvConfig;

        public CsvFileTableWrapper(string filename)
        {
            Filename = filename;

        }
        public string Filename { get; set; }
        public async Task<IEnumerable<KeyValuePair<string, IAsyncEnumerable<RawEntry>>>> GetColumns()
        {
            var columnLocations = new Dictionary<string, int>();
            try
            {
                using var fileStream = File.OpenRead(Filename);
                var reader = CreateCsvReader(fileStream);
                var columnNames = await reader.ReadHeadersAsync();
                for(int i = 0; i < columnNames.Count; i++)
                {
                    columnLocations[columnNames[i]] = i; 
                }
            }
            catch (Exception)
            {
                return GetDefaultValue();
            }
            var columValues = columnLocations.ToDictionary(column => column.Key, column => GetColumnValues(column.Value));
            return columValues;
        }

        private async IAsyncEnumerable<RawEntry> GetColumnValues(int columnLocation)
        {
            var reader = CreateCsvReader(Filename);
            var columnValues = reader.ReadColumn(columnLocation);
            await foreach (var value in columnValues)
            {
                if (value != null)
                    yield return new RawEntry(Filename, value);
            }
        }

        private AsyncCsvReader CreateCsvReader(string filename)
        {
            var stream = File.OpenRead(filename);
            return CreateCsvReader(stream);
        }

        private AsyncCsvReader CreateCsvReader(Stream stream)
        {
            var csvReader = new AsyncCsvReader(stream);
            return csvReader;
        }

        private static IEnumerable<KeyValuePair<string, IAsyncEnumerable<RawEntry>>> GetDefaultValue()
        {
            return Enumerable.Empty<KeyValuePair<string, IAsyncEnumerable<RawEntry>>>();
        }

    }
}
