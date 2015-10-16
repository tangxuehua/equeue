using System.IO;

namespace EQueue.Broker.Storage
{
    internal class WriterWorkItem
    {
        private readonly FileStream _fileStream;

        public readonly MemoryStream BufferStream;
        public readonly BinaryWriter BufferWriter;
        public readonly Stream WorkingStream;
        public long LastFlushedPosition;

        public WriterWorkItem(Stream stream)
        {
            WorkingStream = stream;
            BufferStream = new MemoryStream(8192);
            BufferWriter = new BinaryWriter(BufferStream);
            _fileStream = WorkingStream as FileStream;
        }

        public void AppendData(byte[] buf, int offset, int len)
        {
            WorkingStream.Write(buf, 0, len);
        }
        public void FlushToDisk()
        {
            if (_fileStream != null)
            {
                _fileStream.Flush(true);
                LastFlushedPosition = _fileStream.Position;
            }
        }
        public void ResizeStream(long length)
        {
            WorkingStream.SetLength(length);
        }
        public void Dispose()
        {
            WorkingStream.Dispose();
        }
    }
}
