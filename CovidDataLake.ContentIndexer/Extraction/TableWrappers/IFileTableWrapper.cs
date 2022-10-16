using System.Collections.Generic;
using CovidDataLake.ContentIndexer.Extraction.Models;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers
{
    public interface IFileTableWrapper
    {
        string Filename { get; set; }
        IEnumerable<KeyValuePair<string, IAsyncEnumerable<RawEntry>>> GetColumns();
    }
}
