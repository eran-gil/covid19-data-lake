using System.Collections.Generic;
using CovidDataLake.ContentIndexer.Extraction.Models;
using CovidDataLake.ContentIndexer.Indexing.Models;

namespace CovidDataLake.ContentIndexer.Indexing
{
    public interface IIndexFileAccess
    {
        IList<RootIndexRow> CreateUpdatedIndexFileWithValues(string sourceIndexFileName,
            IList<RawEntry> values
        );
    }
}
