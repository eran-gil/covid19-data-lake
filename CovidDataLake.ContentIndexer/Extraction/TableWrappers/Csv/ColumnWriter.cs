using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Csv
{
    internal class ColumnWriter : IDisposable
    {
        private readonly StreamWriter _streamWriter;
        private readonly HashSet<string> _distinctValues;

        public ColumnWriter(Stream stream)
        {
            _streamWriter = new StreamWriter(stream, Encoding.UTF8, -1, true);
            _distinctValues = new HashSet<string>();
        }

        public Stream BaseStream => _streamWriter.BaseStream;

        public void WriteValue(string value)
        {
            if (value == null || _distinctValues.Contains(value))
            {
                return;
            }
            _streamWriter.WriteLine(value);
            _distinctValues.Add(value);
        }

        public void Dispose()
        {
            _streamWriter.Close();
        }
    }
}
