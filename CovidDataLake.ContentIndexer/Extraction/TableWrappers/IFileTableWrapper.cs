using System;
using System.Collections.Generic;

namespace CovidDataLake.ContentIndexer.Extraction.TableWrappers
{
    public interface IFileTableWrapper : IDisposable
    {
        string Filename { get; set; }
        IEnumerable<KeyValuePair<string, IEnumerable<string>>> GetColumns();
    }
}
