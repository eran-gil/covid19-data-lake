using System.Collections.Generic;
using System.Threading.Tasks;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers
{
    public interface IFileTableWrapper
    {
        string Filename { get; set; }
        Task<IEnumerable<KeyValuePair<string, IAsyncEnumerable<string>>>> GetColumns();
    }
}
