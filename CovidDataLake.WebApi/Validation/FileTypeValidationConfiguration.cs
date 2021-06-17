using System.Collections.Generic;

namespace CovidDataLake.WebApi.Validation
{
    public class FileTypeValidationConfiguration
    {
        public IEnumerable<string> AllowedFileTypes { get; set; }
    }
}
