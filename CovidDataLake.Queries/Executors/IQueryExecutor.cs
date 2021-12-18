using System.Collections.Generic;
using CovidDataLake.Queries.Models;

namespace CovidDataLake.Queries.Executors
{
    public interface IQueryExecutor
    {
        bool CanHandle(string queryType);
        IEnumerable<QueryResult> ExecuteFromString(string query);

    }


    public interface IQueryExecutor<in T> : IQueryExecutor
    {
        IEnumerable<QueryResult> Execute(T query);
    }
}
