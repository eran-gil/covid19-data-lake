namespace CovidDataLake.Common.Locking
{
    public interface ILock
    {
        Task TakeLock(string lockName, TimeSpan lockExpiration);
        Task ReleaseLock(string lockName);
        void ExtendLock(string lockName, TimeSpan lockExpiration);
    }
}
