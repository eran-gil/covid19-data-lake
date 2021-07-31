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
            return _csvReader.HeaderRecord.Select(column =>
            {
                var columnValues = GetColumnValues(column).Where(IsValueNotNull);
                return new KeyValuePair<string, IEnumerable<string>>(column, columnValues);
            });
        }

        private IEnumerable<string> GetColumnValues(string columnName)
        {
            lock (_streamReader)
            {
                while (_csvReader.Read())
                {
                    var record = _csvReader.GetRecord<dynamic>();
                    yield return record.GetType().GetProperty(columnName).GetValue(record, null);
                }

                _streamReader.BaseStream.Seek(0, SeekOrigin.Begin);
            }
        }

        private bool IsValueNotNull(object val)
        {
            return val != null;
        }

        public void Dispose()
        {
            _streamReader?.Dispose();
            _csvReader?.Dispose();
        }
    }
}