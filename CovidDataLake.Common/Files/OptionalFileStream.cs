using System;
using System.IO;

namespace CovidDataLake.Common.Files
{
    public class OptionalFileStream : IDisposable
    {
        private readonly string _filename;
        private readonly bool _shouldDelete;
        public FileStream BaseStream { get; }

        private OptionalFileStream(FileStream baseStream, string filename, bool shouldDelete)
        {
            _filename = filename;
            _shouldDelete = shouldDelete;
            BaseStream = baseStream;
        }

        public static OptionalFileStream CreateOptionalFileReadStream(string filename, bool shouldDelete = true)
        {
            var stream = default(FileStream);
            if (IsFileGoodForReading(filename))
            {
                stream = File.OpenRead(filename);
            }

            return new OptionalFileStream(stream, filename, shouldDelete);
        }

        private static bool IsFileGoodForReading(string filename)
        {
            return File.Exists(filename) && new FileInfo(filename).Length > 0;
        }

        public void Dispose()
        {
            if (BaseStream == default(FileStream)) return;
            BaseStream.Dispose();
            if (_shouldDelete) File.Delete(_filename);
        }
    }
}