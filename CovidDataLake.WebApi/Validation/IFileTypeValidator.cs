namespace CovidDataLake.WebApi.Validation
{
    public interface IFileTypeValidator
    {
        bool IsFileTypeValid(string fileType);
    }
}
