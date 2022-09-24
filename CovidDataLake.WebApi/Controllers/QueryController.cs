using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using CovidDataLake.Queries.Exceptions;
using CovidDataLake.Queries.Executors;
using CovidDataLake.Queries.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using Microsoft.Extensions.Logging;

namespace CovidDataLake.WebApi.Controllers
{

    [Route("api/v1/[controller]")]
    [ApiController]
    public class QueryController : Controller
    {
        private readonly IEnumerable<IQueryExecutor> _queryExecutors;
        private readonly ILogger<QueryController> _logger;

        public QueryController(IEnumerable<IQueryExecutor> queryExecutors, ILogger<QueryController> logger)
        {
            _queryExecutors = queryExecutors;
            _logger = logger;
        }

        [HttpGet]
        public ActionResult Get()
        {
            return Ok("ok");
        }

        [HttpPost]
        public async Task<ActionResult<IEnumerable<QueryResult>>> Post([BindRequired][FromQuery(Name = "queryType")] string queryType,
             [BindRequired][FromBody] JsonDocument queryBody)
        {
            var querySession = Guid.NewGuid();
            var loggingProperties =
                new Dictionary<string, object> { ["SessionId"] = querySession, ["QueryType"] = queryType };
            using var scope = _logger.BeginScope(loggingProperties);
            _logger.LogInformation($"query-start");
            var relevantQueryExecutor = _queryExecutors.FirstOrDefault(executor => executor.CanHandle(queryType));
            if (relevantQueryExecutor == default(IQueryExecutor))
            {
                return BadRequest($"Invalid query type {queryType}");
            }


            ObjectResult result;
            var resultCount = 0;
            try
            {
                var results = (await relevantQueryExecutor.ExecuteFromString(queryBody)).ToList();
                resultCount = results.Count;
                result = Ok(results);
            }

            catch (InvalidQueryFormatException)
            {
                result = BadRequest($"Invalid query format for query type {queryType}");
            }
            catch (Exception)
            {
                result = StatusCode(500, "An error occurred while trying to perform your query");
            }
            var resultLoggingProperties =
                new Dictionary<string, object> { ["ResultCount"] = resultCount};
            using var resultScope = _logger.BeginScope(resultLoggingProperties);
            _logger.LogInformation($"query-end");
            return result;
        }
    }
}
