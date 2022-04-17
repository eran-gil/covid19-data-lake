using System.Collections.Generic;
using System.Threading.Tasks;
using CovidDataLake.ContentIndexer.Extraction.Models;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public interface IIndexFileWriter
    {
        Task<IEnumerable<RootIndexRow>> UpdateIndexFileWithValues(IList<RawEntry> values, string indexFilename);
    }
}
