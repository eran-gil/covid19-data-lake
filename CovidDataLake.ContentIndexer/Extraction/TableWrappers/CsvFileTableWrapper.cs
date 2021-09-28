using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers
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
        public async Task<IEnumerable<KeyValuePair<string, IAsyncEnumerable<string>>>> GetColumns()
        {
            //todo: handle null header record
            using var reader = CreateCsvReader(Filename);
            await reader.ReadAsync();
            reader.ReadHeader();
            return reader.HeaderRecord.ToDictionary(column => column, GetColumnValues);
        }

        private async IAsyncEnumerable<string> GetColumnValues(string columnName)
        {
            using var reader = CreateCsvReader(Filename);
            var records = reader.GetRecordsAsync<dynamic>();
            await foreach (var record in records)
            {
                var recordDictionary = record as IDictionary<string, object>;
                var propValue = recordDictionary?[columnName];
                if (propValue != null)
                    yield return propValue.ToString();
            }
        }

        private CsvReader CreateCsvReader(string filename)
        {
            var streamReader = new StreamReader(filename);
            var csvReader = new CsvReader(streamReader, _csvConfig);
            return csvReader;
        }

    }
}