using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CovidDataLake.DAL.Utils;
using CovidDataLake.DAL.Write;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;

namespace CovidDataLake.WebApi.Controllers
{

    [Route("api/v1/[controller]")]
    [ApiController]
    public class DataLakeController : ControllerBase
    {
        private readonly IDataLakeWriter _dataLakeWriter;

        public DataLakeController(IDataLakeWriter dataLakeWriter)
        {
            _dataLakeWriter = dataLakeWriter;
        }

        [HttpPost]
        [Route("PostNewFile")]
        public async Task<ActionResult> PostNewFile([BindRequired] [FromQuery(Name = "file_path")] string fileName)
        {
            using (var stream = _dataLakeWriter.CreateFileStream(fileName.GetExtensionFromPath()))
            {
                try
                {
                    await Request.Body.CopyToAsync(stream);
                }
                catch (Exception e)
                {
                    //TODO: add logging
                    return StatusCode(500, e);
                }
            }
            
            return Ok();
        }
    }
}
