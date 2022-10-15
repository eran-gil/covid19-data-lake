using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using DataAccess;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Csv
{
    public class CsvFileReader : IDisposable
    {
        private readonly StreamReader _stream;
        private readonly string _separator;
        private bool _headerRead;
        private readonly DataTable _dataTable;

        public CsvFileReader(Stream stream, string separator = ",")
        {
            _stream = new StreamReader(stream);
            _dataTable = DataTable.New.ReadLazy(stream);
            _separator = separator;
            _headerRead = false;
        }

        public IList<string> ReadHeaders()
        {
            var headers = _dataTable.ColumnNames.ToList();
            _headerRead = true;
            return headers;
        }

        public IEnumerable<IList<string>> ReadLines()
        {
            return _dataTable.Rows.Select(row => row.Values);
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
