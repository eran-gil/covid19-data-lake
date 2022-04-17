using System.Collections.Generic;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Extraction.Models;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers
{
    public interface IFileTableWrapper
    {
        string Filename { get; set; }
        Task<IEnumerable<KeyValuePair<string, IAsyncEnumerable<RawEntry>>>> GetColumns();
    }
}
