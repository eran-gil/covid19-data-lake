using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers.Csv
{
    internal class ColumnWriter
    {
        private readonly HashSet<string> _distinctValues;
        private readonly Channel<string> _columnValues;

        public ColumnWriter()
        {
            _columnValues = Channel.CreateUnbounded<string>();
            _distinctValues = new HashSet<string>();
        }

        public IAsyncEnumerable<string> ColumnValues => _columnValues.Reader.ReadAllAsync();

        public async Task WriteValue(string value)
        {
            if (string.IsNullOrEmpty(value) || _distinctValues.Contains(value))
            {
                return;
            }
            await _columnValues.Writer.WriteAsync(value);
            _distinctValues.Add(value);
        }

        public void FinishColumn()
        {
            _columnValues.Writer.Complete();
        }

    }
}
