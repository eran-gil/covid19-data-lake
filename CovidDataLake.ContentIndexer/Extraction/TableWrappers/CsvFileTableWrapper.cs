using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers
{
    class CsvFileTableWrapper : IFileTableWrapper
    {
        private readonly StreamReader _streamReader;
        private readonly CsvReader _csvReader;
        private IEnumerable<dynamic> _records;
        private readonly object _lockObject = new();

        public CsvFileTableWrapper(string filename)
        {
            Filename = filename;
            var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                NewLine = Environment.NewLine,
                HasHeaderRecord = true
            };
            _streamReader = new StreamReader(filename);
            _csvReader = new CsvReader(_streamReader, csvConfig);
        }
        public string Filename { get; set; }
        public IEnumerable<KeyValuePair<string, IEnumerable<string>>> GetColumns()
        {
            //todo: handle null header record
            _csvReader.Read();
            _csvReader.ReadHeader();
            return _csvReader.HeaderRecord.ToDictionary(column => column, GetColumnValues);
        }

        private IEnumerable<string> GetColumnValues(string columnName)
        {
            lock (_lockObject)
            {
                _records ??= _csvReader.GetRecords<dynamic>();
                foreach (var record in _records)
                {
                    var recordDictionary = record as IDictionary<string, object>;
                    var propValue = recordDictionary?[columnName];
                    if (propValue != null)
                        yield return propValue.ToString();
                }
            }
        }

        public void Dispose()
        {
            _streamReader?.Dispose();
            _csvReader?.Dispose();
        }
    }
}