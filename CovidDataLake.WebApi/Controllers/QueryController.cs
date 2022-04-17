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
        public async Task<ActionResult<IEnumerable<QueryResult>>> Post([BindRequired][FromQuery(Name = "queryType")] string queryType,
             [BindRequired][FromBody] object queryBody)
        {
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

            try
            {
                var results = await relevantQueryExecutor.ExecuteFromString(body);
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