using System;

namespace CovidDataLake.Common.Locking
{
    public interface ILock
    {
        void TakeLock(string lockName, TimeSpan lockExpiration);
        void ReleaseLock(string lockName);
        void ExtendLock(string lockName, TimeSpan lockExpiration);
    }
}
