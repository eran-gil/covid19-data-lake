using System.Collections.Generic;

namespace CovidDataLake.ContentIndexer.TableWrappers
{
    public interface IFileTableWrapper
    {
        string Filename { get; set; }
        IEnumerable<KeyValuePair<string, IEnumerable<string>>> GetColumns();
    }
}
