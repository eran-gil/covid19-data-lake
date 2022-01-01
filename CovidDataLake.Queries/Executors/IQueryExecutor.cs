using System.Collections.Generic;
using System.Threading.Tasks;
using CovidDataLake.Queries.Models;

namespace CovidDataLake.Queries.Executors
{
    public interface IQueryExecutor
    {
        bool CanHandle(string queryType);
        Task<IEnumerable<QueryResult>> ExecuteFromString(string query);

    }


    public interface IQueryExecutor<in T> : IQueryExecutor
    {
        Task<IEnumerable<QueryResult>> Execute(T query);
    }
}
