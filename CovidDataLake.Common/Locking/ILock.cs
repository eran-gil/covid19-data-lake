using System;
using System.Threading.Tasks;

namespace CovidDataLake.Common.Locking
{
    public interface ILock
    {
        Task<bool> TakeLockAsync(string lockName, TimeSpan lockExpiration);
        Task<bool> ReleaseLockAsync(string lockName);
    }
}
