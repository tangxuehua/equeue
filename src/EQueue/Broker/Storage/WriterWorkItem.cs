using System.IO;

namespace EQueue.Broker.Storage
{
    internal class WriterWorkItem
    {
        private readonly FileStream _fileStream;
        private UnmanagedMemoryStream _memStream;
        private Stream _workingStream;

        public readonly MemoryStream BufferStream;
        public readonly BinaryWriter BufferWriter;
        public Stream WorkingStream { get { return _workingStream; } }

        public WriterWorkItem(FileStream fileStream, UnmanagedMemoryStream memStream)
        {
            _fileStream = fileStream;
            _memStream = memStream;
            _workingStream = (Stream)fileStream ?? memStream;
            BufferStream = new MemoryStream(8192);
            BufferWriter = new BinaryWriter(BufferStream);
        }

        public void SetMemStream(UnmanagedMemoryStream memStream)
        {
            _memStream = memStream;
            if (_fileStream == null)
            {
                _workingStream = memStream;
            }
        }
        public void AppendData(byte[] buf, int offset, int len)
        {
            var fileStream = _fileStream;
            if (fileStream != null)
            {
                fileStream.Write(buf, 0, len);
            }
            var memStream = _memStream;
            if (memStream != null)
            {
                memStream.Write(buf, 0, len);
            }
        }
        public void FlushToDisk()
        {
            if (_fileStream != null)
            {
                _fileStream.FlushToDisk();
            }
        }
        public void ResizeStream(long fileSize)
        {
            var fileStream = _fileStream;
            if (fileStream != null)
            {
                fileStream.SetLength(fileSize);
            }
            var memStream = _memStream;
            if (memStream != null)
            {
                memStream.SetLength(fileSize);
            }
        }

        public void Dispose()
        {
            var fileStream = _fileStream;
            if (fileStream != null)
            {
                fileStream.Dispose();
            }
            DisposeMemStream();
        }
        public void DisposeMemStream()
        {
            var memStream = _memStream;
            if (memStream != null)
            {
                memStream.Dispose();
                _memStream = null;
            }
        }
    }
}
