using System;
using System.Security.Cryptography;
using System.Text;

namespace CovidDataLake.Common.Hashing
{
    public class Md5StringHash : IStringHash
    {
        private readonly MD5 _hasher;

        public Md5StringHash()
        {
            _hasher = MD5.Create();
        }

        public ulong HashStringToUlong(string value)
        {
            var hashed = _hasher.ComputeHash(Encoding.UTF8.GetBytes(value));
            var hashedValue = BitConverter.ToUInt64(hashed, 0);
            return hashedValue;
        }
    }
}
