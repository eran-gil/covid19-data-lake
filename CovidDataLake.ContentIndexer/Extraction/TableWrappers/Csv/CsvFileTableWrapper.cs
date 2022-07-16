using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Extraction.Models;
using CsvHelper;
using CsvHelper.Configuration;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Csv
{
    class CsvFileTableWrapper : IFileTableWrapper
    {
        private readonly CsvConfiguration _csvConfig;

        public CsvFileTableWrapper(string filename)
        {
            Filename = filename;
            _csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                NewLine = Environment.NewLine,
                HasHeaderRecord = true
            };

        }
        public string Filename { get; set; }
        public async Task<IEnumerable<KeyValuePair<string, IAsyncEnumerable<RawEntry>>>> GetColumns()
        {
            using var reader = CreateCsvReader(Filename);
            var canRead = await reader.ReadAsync();
            if (!canRead) return GetDefaultValue();
            try
            {
                reader.ReadHeader();
            }
            catch (Exception)
            {
                return GetDefaultValue();
            }
            return reader.HeaderRecord.ToDictionary(column => column, GetColumnValues);
        }

        private async IAsyncEnumerable<RawEntry> GetColumnValues(string columnName)
        {
            using var reader = CreateCsvReader(Filename);
            var records = reader.GetRecordsAsync<dynamic>();
            await foreach (var record in records)
            {
                var recordDictionary = record as IDictionary<string, object>;
                var propValue = recordDictionary?[columnName];
                if (propValue != null)
                    yield return new RawEntry(Filename, propValue.ToString());
            }
        }

        private CsvReader CreateCsvReader(string filename)
        {
            var streamReader = new StreamReader(filename);
            var csvReader = new CsvReader(streamReader, _csvConfig);
            return csvReader;
        }

        private static IEnumerable<KeyValuePair<string, IAsyncEnumerable<RawEntry>>> GetDefaultValue()
        {
            return Enumerable.Empty<KeyValuePair<string, IAsyncEnumerable<RawEntry>>>();
        }

    }
}
