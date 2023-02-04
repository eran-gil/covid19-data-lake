using System;
using Newtonsoft.Json;

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

        [JsonProperty(Order = 1)]
        public string ColumnName { get; set; }
        [JsonProperty(Order = 2)]
        public string Min { get; set; }
        [JsonProperty(Order = 3)]
        public string Max { get; set; }
        [JsonProperty(Order = 4)]
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
