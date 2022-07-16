﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
             [BindRequired][FromBody] object queryBody)
        {
            var querySession = Guid.NewGuid();
            _logger.LogInformation($"Query {querySession} of type {queryType} has begun");
            var relevantQueryExecutor = _queryExecutors.FirstOrDefault(executor => executor.CanHandle(queryType));
            if (relevantQueryExecutor == default(IQueryExecutor))
            {
                return BadRequest($"Invalid query type {queryType}");
            }

            string body;
            Request.Body.Seek(0, SeekOrigin.Begin);
            using (var reader = new StreamReader(Request.Body))
            {
                body = await reader.ReadToEndAsync();
            }

            ObjectResult result;
            try
            {
                var results = await relevantQueryExecutor.ExecuteFromString(body);
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
            _logger.LogInformation($"Query {querySession} of type {queryType} has finished");
            return result;
        }
    }
}
