using System.Collections.Generic;

namespace CovidDataLake.Queries.Models
{
    public class NeedleInHaystackQuery
    {
        public ConditionRelation Relation { get; set; }
        public IEnumerable<NeedleInHaystackColumnCondition> Conditions { get; set; }
    }
}
