using System;
using System.Threading.Tasks;

namespace CovidDataLake.DAL.Pubsub
{
    public interface IProducer : IDisposable
    {
        Task<bool> SendMessage(string filename);
    }
}