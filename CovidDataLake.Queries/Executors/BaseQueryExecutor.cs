using System;
using System.Collections.Generic;
using CovidDataLake.Queries.Exceptions;
using CovidDataLake.Queries.Models;
using Jil;

namespace CovidDataLake.Queries.Executors
{
    public abstract class BaseQueryExecutor<T> : IQueryExecutor<T>
    {
        public IEnumerable<QueryResult> ExecuteFromString(string query)
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
            return Execute(queryObject);

        }

        public abstract IEnumerable<QueryResult> Execute(T query);

        public abstract bool CanHandle(string queryType);

        
    }
}
