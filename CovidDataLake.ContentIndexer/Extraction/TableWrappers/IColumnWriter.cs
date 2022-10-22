using System.Collections.Generic;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Extraction.Models;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers;

internal interface IColumnWriter
{
    void WriteValue(string value);
    Task WriteValueAsync(string value);
    IAsyncEnumerable<RawEntry> GetColumnEntries(List<StringWrapper> originFileNames);
    void FinishWriting();
}
