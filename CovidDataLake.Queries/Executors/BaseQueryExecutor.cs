using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using CovidDataLake.Cloud.Amazon;
using CovidDataLake.Cloud.Amazon.Configuration;
using CovidDataLake.Queries.Exceptions;
using CovidDataLake.Queries.Models;

namespace CovidDataLake.Queries.Executors
{
    public abstract class BaseQueryExecutor<T> : IQueryExecutor<T>
    {
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly string _bucketName;

        protected BaseQueryExecutor(IAmazonAdapter amazonAdapter, BasicAmazonIndexFileConfiguration config)
        {
            _amazonAdapter = amazonAdapter;
            _bucketName = config.BucketName;
        }

        public async Task<IEnumerable<QueryResult>> ExecuteFromString(JsonDocument query)
        {
            T queryObject;
            try
            {
                queryObject = query.Deserialize<T>();
            }
            catch (Exception)
            {
                throw new InvalidQueryFormatException();
            }
            return await Execute(queryObject);

        }

        public abstract Task<IEnumerable<QueryResult>> Execute(T query);

        public abstract bool CanHandle(string queryType);

        protected async Task<string> DownloadIndexFile(string filename)
        {
            try
            {
                return await _amazonAdapter.DownloadObjectAsync(_bucketName, filename);
            }
            catch (ResourceNotFoundException)
            {
                return null;
            }

        }

    }
}
