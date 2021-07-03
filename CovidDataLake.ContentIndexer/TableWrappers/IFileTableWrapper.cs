using System.Collections.Generic;

namespace CovidDataLake.ContentIndexer.TableWrappers
{
    public interface IFileTableWrapper
    {
        Dictionary<string, IList<string>> GetColumns();
    }
}
