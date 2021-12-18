using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CovidDataLake.Queries.Exceptions;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using CovidDataLake.Queries.Executors;
using CovidDataLake.Queries.Models;

namespace CovidDataLake.WebApi.Controllers
{

    [Route("api/v1/[controller]")]
    [ApiController]
    public class QueryController : Controller
    {
        private readonly IEnumerable<IQueryExecutor> _queryExecutors;

        public QueryController(IEnumerable<IQueryExecutor> queryExecutors)
        {
            _queryExecutors = queryExecutors;
        }

        [HttpGet]
        public ActionResult Get()
        {
            return Ok("ok");
        }

        [HttpPost]
        public ActionResult<IEnumerable<QueryResult>> Post([BindRequired] [FromQuery(Name = "queryType")] string queryType,
             [BindRequired][FromBody] object queryBody)
        {
            var relevantQueryExecutor = _queryExecutors.FirstOrDefault(executor => executor.CanHandle(queryType));
            if (relevantQueryExecutor == default(IQueryExecutor))
            {
                return BadRequest($"Invalid query type {queryType}");
            }

            string body;
            using (var reader = new StreamReader(Request.Body))
            {
                body = reader.ReadToEnd();
            }

            try
            {
                var results = relevantQueryExecutor.ExecuteFromString(body);
                return Ok(results);
            }

            catch (InvalidQueryFormatException)
            {
                return BadRequest($"Invalid query format for query type {queryType}");
            }
            catch (Exception)
            {
                return StatusCode(500, "An error occurred while trying to perform your query");

            }
        }
    }
}
