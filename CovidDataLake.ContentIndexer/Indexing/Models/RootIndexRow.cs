using System;

namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class RootIndexRow : IComparable<RootIndexRow>
    {

        public RootIndexRow()
        {
        }

        public RootIndexRow(string columnName, string min, string max, string fileName)
        {
            ColumnName = columnName;
            Min = min;
            Max = max;
            FileName = fileName;
        }

        public string ColumnName { get; set; }
        public string Min { get; set; }
        public string Max { get; set; }
        public string FileName { get; set; }

        public int CompareTo(RootIndexRow other)
        {
            if (ReferenceEquals(this, other))
            {
                return 0;
            }

            if (ReferenceEquals(null, other))
            {
                return 1;
            }

            var columnNameComparison = string.Compare(ColumnName, other.ColumnName, StringComparison.Ordinal);
            if (columnNameComparison != 0)
            {
                return columnNameComparison;
            }

            return string.Compare(Max, other.Max, StringComparison.Ordinal);
        }
    }
}
