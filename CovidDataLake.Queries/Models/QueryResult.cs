using System.Collections.Generic;

namespace CovidDataLake.Queries.Models
{
    public class QueryResult
    {
        public QueryResult(string fileName, IEnumerable<string> hitValues)
        {
            FileName = fileName;
            HitValues = hitValues;
        }
        public string FileName { get; set; }
        public IEnumerable<string> HitValues { get; set; }
    }
}
