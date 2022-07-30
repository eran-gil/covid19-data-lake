using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Csv
{
    public class AsyncCsvReader : IDisposable
    {
        private readonly StreamReader _stream;
        private readonly string _separator;
        private bool _headerRead;

        public AsyncCsvReader(Stream stream, string separator = ",")
        {
            _stream = new StreamReader(stream);
            _separator = separator;
            _headerRead = false;
        }

        public async Task<IList<string>> ReadHeadersAsync()
        {
            var headerLine = await _stream.ReadLineAsync();
            var headers = headerLine.Split(_separator);
            _headerRead = true;
            return headers;
        }

        public async IAsyncEnumerable<string> ReadColumn(int index)
        {
            if (!_headerRead)
            {
                await ReadHeadersAsync();
            }
            while(_stream.BaseStream.Position < _stream.BaseStream.Length)
            {
                var row = await _stream.ReadLineAsync();
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
