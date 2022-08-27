using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Csv
{
    public class CsvFileReader : IDisposable
    {
        private readonly StreamReader _stream;
        private readonly string _separator;
        private bool _headerRead;

        public CsvFileReader(Stream stream, string separator = ",")
        {
            _stream = new StreamReader(stream);
            _separator = separator;
            _headerRead = false;
        }

        public IList<string> ReadHeaders()
        {
            var headerLine = _stream.ReadLine();
            var headers = headerLine?.Split(_separator);
            _headerRead = true;
            return headers;
        }

        public IEnumerable<string> ReadColumn(int index)
        {
            if (!_headerRead)
            {
                ReadHeaders();
            }
            while(!_stream.EndOfStream)
            {
                var row = _stream.ReadLine();
                if (row == null)
                {
                    continue;
                }

                var rowValues = row.Split(_separator);
                yield return rowValues.ElementAtOrDefault(index);
            }
        }
        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}
