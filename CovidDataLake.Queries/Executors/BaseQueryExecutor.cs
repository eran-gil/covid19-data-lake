using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CovidDataLake.Queries.Exceptions;
using CovidDataLake.Queries.Models;
using Jil;

namespace CovidDataLake.Queries.Executors
{
    public abstract class BaseQueryExecutor<T> : IQueryExecutor<T>
    {
        public async Task<IEnumerable<QueryResult>> ExecuteFromString(string query)
        {
            T queryObject;
            try
            {
                queryObject = JSON.Deserialize<T>(query);
            }
            catch (Exception)
            {
                throw new InvalidQueryFormatException();
            }
            return await Execute(queryObject);

        }

        public abstract Task<IEnumerable<QueryResult>> Execute(T query);

        public abstract bool CanHandle(string queryType);


    }
}
