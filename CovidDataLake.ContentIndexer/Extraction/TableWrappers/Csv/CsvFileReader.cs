using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NReco.Csv;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Csv
{
    public class CsvFileReader : IDisposable
    {
        private readonly StreamReader _streamReader;
        private readonly CsvReader _csvReader;

        public CsvFileReader(Stream stream)
        {
            _streamReader = new StreamReader(stream);
            _csvReader = new CsvReader(_streamReader);
        }

        public IList<string> ReadHeaders()
        {
            _csvReader.Read();
            var headers = Enumerable.Range(0, _csvReader.FieldsCount)
                .Select(i => _csvReader[i])
                .ToList();

            return headers;
        }

        public IEnumerable<IList<string>> ReadLines()
        {
            while (_csvReader.Read())
            {
                yield return Enumerable.Range(0, _csvReader.FieldsCount)
                    .Select(i => _csvReader[i])
                    .ToList();
            }
        }
        public void Dispose()
        {
            _streamReader.Dispose();
        }
    }
}
