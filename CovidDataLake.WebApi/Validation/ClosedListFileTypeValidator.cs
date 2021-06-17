using System.Collections.Generic;

namespace CovidDataLake.WebApi.Validation
{
    public class ClosedListFileTypeValidator : IFileTypeValidator
    {
        private readonly HashSet<string> _allowedFileTypes;

        public ClosedListFileTypeValidator(FileTypeValidationConfiguration configuration)
        {
            //TODO: take file types from configuration
            _allowedFileTypes = new HashSet<string>(configuration.AllowedFileTypes);
        }
        public bool IsFileTypeValid(string fileType)
        {
            return !string.IsNullOrEmpty(fileType) && _allowedFileTypes.Contains(fileType.ToLowerInvariant());
        }
    }
}
