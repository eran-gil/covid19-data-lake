using System;
using System.Net;
using System.Threading.Tasks;
using CovidDataLake.DAL.Pubsub;
using CovidDataLake.DAL.Utils;
using CovidDataLake.DAL.Write;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;

namespace CovidDataLake.WebApi.Controllers
{

    [Route("api/v1/[controller]")]
    [ApiController]
    public class DataLakeController : Controller
    {
        private readonly IDataLakeWriter _dataLakeWriter;
        private readonly IProducer _messageProducer;

        public DataLakeController(IDataLakeWriter dataLakeWriter, IProducerFactory producerFactory)
        {
            _dataLakeWriter = dataLakeWriter;
            _messageProducer = producerFactory.CreateProducer(Dns.GetHostName());
        }

        [HttpPost]
        [Route("PostNewFile")]
        public async Task<ActionResult> PostNewFile([BindRequired] [FromQuery(Name = "filename")] string filename)
        {
            var fileType = filename.GetExtensionFromPath();
            using (var stream = _dataLakeWriter.CreateFileStream(fileType, out var outputFilepath))
            {
                try
                {
                    await Request.Body.CopyToAsync(stream);
                    var result = await _messageProducer.SendMessage(outputFilepath);
                    if (!result)
                    {
                        await _dataLakeWriter.DeleteFileAsync(outputFilepath);
                        return StatusCode(500, "Failed to send the file to kafka, the file was deleted from the data lake");
                    }
                }
                catch (Exception e)
                {
                    //TODO: add logging
                    return StatusCode(500, e);
                }
            }
            return Ok();
        }

        protected override void Dispose(bool disposing)
        {
            _messageProducer.Dispose();
            base.Dispose(disposing);
        }
    }
}
