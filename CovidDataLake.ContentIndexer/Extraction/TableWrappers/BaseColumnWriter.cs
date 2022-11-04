using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Extraction.Models;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers
{
    internal abstract class BaseColumnWriter : IColumnWriter
    {
        private readonly HashSet<string> _distinctValues;

        protected BaseColumnWriter()
        {
            _distinctValues = new HashSet<string>();
        }

        public abstract void WriteValue(string value);

        public abstract Task WriteValueAsync(string value);

        public abstract IAsyncEnumerable<RawEntry> GetColumnEntries(ImmutableHashSet<StringWrapper> originFileNames);

        public virtual void FinishWriting()
        {
            _distinctValues.Clear();
        }

        protected bool ShouldWriteValue(string value)
        {
            return !string.IsNullOrEmpty(value) && !_distinctValues.Contains(value);
        }

        protected void AddDistinctValue(string value)
        {
            _distinctValues.Add(value);
        }

        protected static IAsyncEnumerable<RawEntry> GetFilteredEntries(IAsyncEnumerable<string> values, ImmutableHashSet<StringWrapper> originFileNames)
        {
            return values.Select(value => new RawEntry(originFileNames, value));
        }
    }
}
