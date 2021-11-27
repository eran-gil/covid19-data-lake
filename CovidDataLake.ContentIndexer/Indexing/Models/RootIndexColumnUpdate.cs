using System;
using System.Collections.Generic;

namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class RootIndexColumnUpdate : IComparable<RootIndexColumnUpdate>
    {
        public string ColumnName { get; set; }
        public SortedSet<RootIndexRow> Rows { get; set; }

        public int CompareTo(RootIndexColumnUpdate other)
        {
            if (ReferenceEquals(this, other)) return 0;
            if (ReferenceEquals(null, other)) return 1;
            return string.Compare(ColumnName, other.ColumnName, StringComparison.Ordinal);
        }
    }
}
