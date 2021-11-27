namespace CovidDataLake.ContentIndexer.Indexing.Models
{
    public class RootIndexRow
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

    }
}
