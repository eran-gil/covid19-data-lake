using System;
using System.IO;

namespace CovidDataLake.Common.Files
{
    public class OptionalFileStream : IDisposable
    {
        public FileStream BaseStream { get; }

        private OptionalFileStream(FileStream baseStream)
        {
            BaseStream = baseStream;
        }

        public static OptionalFileStream CreateOptionalFileReadStream(string filename)
        {
            var stream = default(FileStream);
            if (IsFileGoodForReading(filename))
            {
                stream = File.OpenRead(filename);
            }

            return new OptionalFileStream(stream);
        }

        private static bool IsFileGoodForReading(string filename)
        {
            return File.Exists(filename) && new FileInfo(filename).Length > 0;
        }

        public void Dispose()
        {
            BaseStream?.Dispose();
        }
    }
}
