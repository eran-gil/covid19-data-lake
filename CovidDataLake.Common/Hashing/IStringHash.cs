namespace CovidDataLake.Common.Hashing
{
    public interface IStringHash
    {
        ulong HashStringToUlong(string value);
    }
}