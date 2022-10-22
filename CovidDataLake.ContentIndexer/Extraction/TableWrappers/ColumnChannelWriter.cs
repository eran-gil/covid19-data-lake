using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Extraction.Models;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers
{
    internal class ColumnChannelWriter : BaseColumnWriter
    {
        private readonly Channel<string> _columnValues;

        public ColumnChannelWriter()
        {
            _columnValues = Channel.CreateUnbounded<string>();
        }

        public ColumnChannelWriter(int capacity)
        {
            _columnValues = Channel.CreateBounded<string>(capacity);
        }

        public override void WriteValue(string value)
        {
            if (!ShouldWriteValue(value))
            {
                return;
            }

            var success = false;
            while (!success)
            {
                success = _columnValues.Writer.TryWrite(value);
            }

            AddDistinctValue(value);
        }

        public override async Task WriteValueAsync(string value)
        {
            if (!ShouldWriteValue(value))
            {
                return;
            }
            await _columnValues.Writer.WriteAsync(value).ConfigureAwait(false);
            AddDistinctValue(value);
        }

        public override IAsyncEnumerable<RawEntry> GetColumnEntries(List<StringWrapper> originFileNames)
        {
            var columnValues = _columnValues.Reader.ReadAllAsync();
            return GetFilteredEntries(columnValues, originFileNames);
        }

        public override void FinishWriting()
        {
            _columnValues.Writer.Complete();
            base.FinishWriting();
        }

    }
}
