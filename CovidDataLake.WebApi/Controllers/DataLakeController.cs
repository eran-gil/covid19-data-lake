using System;
using System.Net;
using System.Threading.Tasks;
using CovidDataLake.Kafka.Producer;
using CovidDataLake.Storage.Utils;
using CovidDataLake.Storage.Write;
using CovidDataLake.WebApi.Validation;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;

namespace CovidDataLake.WebApi.Controllers
{

    [Route("api/v1/[controller]")]
    [ApiController]
    public class DataLakeController : Controller
    {
        private readonly IDataLakeWriter _dataLakeWriter;
        private readonly IFileTypeValidator _fileTypeValidator;
        private readonly IProducer _messageProducer;

        public DataLakeController(IDataLakeWriter dataLakeWriter, IProducerFactory producerFactory, IFileTypeValidator fileTypeValidator)
        {
            _dataLakeWriter = dataLakeWriter;
            _fileTypeValidator = fileTypeValidator;
            _messageProducer = producerFactory.CreateProducer(Dns.GetHostName());
        }

        [HttpPost]
        [Route("PostNewFile")]
        public async Task<ActionResult> PostNewFile([BindRequired][FromQuery(Name = "filename")] string filename)
        {
            var fileType = filename.GetExtensionFromPath();
            if (!_fileTypeValidator.IsFileTypeValid(fileType))
            {
                return BadRequest("The file type is not supported in this data lake.");
            }

            var outputFilepath = _dataLakeWriter.GenerateFilePath(fileType);
            using (var stream = _dataLakeWriter.CreateFileStream(outputFilepath))
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
            var result = await _messageProducer.SendMessage(outputFilepath);
            if (result) return Ok();

            await _dataLakeWriter.DeleteFileAsync(outputFilepath);
            return StatusCode(500, "Failed to send the file to kafka, the file was deleted from the data lake");
        }

        protected override void Dispose(bool disposing)
        {
            _messageProducer.Dispose();
            base.Dispose(disposing);
        }
    }
}
