using System.Collections.Generic;

namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class RootIndexColumnUpdate
    {
        public string ColumnName { get; set; }
        public IEnumerable<RootIndexRow> Rows { get; set; }
    }
}
