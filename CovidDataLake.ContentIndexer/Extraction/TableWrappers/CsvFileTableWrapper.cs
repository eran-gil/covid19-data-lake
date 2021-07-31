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

        public CsvFileTableWrapper(string filename)
        {
            Filename = filename;
            var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                NewLine = Environment.NewLine,
            };
            _streamReader = new StreamReader(filename);
            _csvReader = new CsvReader(_streamReader, csvConfig);
        }
        public string Filename { get; set; }
        public IEnumerable<KeyValuePair<string, IEnumerable<string>>> GetColumns()
        {
            return _csvReader.HeaderRecord.ToDictionary(column => column, GetColumnValues);
        }

        private IEnumerable<string> GetColumnValues(string columnName)
        {
            lock (_streamReader)
            {
                while (_csvReader.Read())
                {
                    var record = _csvReader.GetRecord<dynamic>();
                    var propValue = record.GetType().GetProperty(columnName).GetValue(record, null);
                    if (propValue != null)
                        yield return propValue;
                }

                _streamReader.BaseStream.Seek(0, SeekOrigin.Begin);
            }
        }

        public void Dispose()
        {
            _streamReader?.Dispose();
            _csvReader?.Dispose();
        }
    }
}