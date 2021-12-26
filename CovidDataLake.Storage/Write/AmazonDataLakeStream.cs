using System.IO;
using CovidDataLake.Cloud.Amazon;

namespace CovidDataLake.Storage.Write
{
    public class AmazonDataLakeStream : Stream
    {
        private readonly IAmazonAdapter _amazonAdapter;
        private readonly FileStream _fileStream;
        private readonly string _bucketName;
        private readonly string _targetFilename;
        private bool _writtenToAmazon;
        private readonly object _lockObject = new object();

        public AmazonDataLakeStream(IAmazonAdapter amazonAdapter, FileStream fileStream, string bucketName, string targetFilename)
        {
            _amazonAdapter = amazonAdapter;
            _fileStream = fileStream;
            _bucketName = bucketName;
            _targetFilename = targetFilename;
            _writtenToAmazon = false;
        }

        public override void Flush()
        {
            _fileStream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return _fileStream.Read(buffer, offset, count);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _fileStream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _fileStream.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _fileStream.Write(buffer, offset, count);
        }

        public override bool CanRead => _fileStream.CanRead;

        public override bool CanSeek => _fileStream.CanSeek;

        public override bool CanWrite => _fileStream.CanWrite;

        public override long Length => _fileStream.Length;

        public override long Position
        {
            get => _fileStream.Position;
            set => _fileStream.Position = value;
        }

        public override void Close()
        {
            _fileStream.Close();
            WriteToAmazon();
        }

        protected override void Dispose(bool disposing)
        {
            _fileStream.Dispose();
            WriteToAmazon();
        }

        private void WriteToAmazon()
        {
            if (!_writtenToAmazon)
            {
                lock (_lockObject)
                {
                    _amazonAdapter.UploadObjectAsync(_bucketName, _targetFilename, _fileStream.Name);
                }
            }
            _writtenToAmazon = true;
        }
    }
}
