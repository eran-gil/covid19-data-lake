using System.Collections.Generic;
using CovidDataLake.Queries.Models;

namespace CovidDataLake.Queries.Executors
{
    interface IQueryExecutor<in T>
    {
        IEnumerable<QueryResult> Execute(T query);
    }
}
