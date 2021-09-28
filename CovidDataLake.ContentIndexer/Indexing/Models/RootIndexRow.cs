namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class RootIndexRow
    {

        public RootIndexRow()
        {
        }

        public RootIndexRow(string columnName, ulong min, ulong max, string fileName)
        {
            ColumnName = columnName;
            Min = min;
            Max = max;
            FileName = fileName;
        }

        public string ColumnName { get; set; }
        public ulong Min { get; set; }
        public ulong Max{ get; set; }
        public string FileName { get; set; }
    }
}
