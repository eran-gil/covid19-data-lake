using System.Collections.Generic;

namespace CovidDataLake.ContentIndexer.TableWrappers
{
    public interface IFileTableWrapper
    {
        string Filename { get; set; }
        Dictionary<string, IList<string>> GetColumns();
    }
}
