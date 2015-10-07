using System.IO;

namespace EQueue.Broker.Storage
{
    internal class WriterWorkItem
    {
        public readonly MemoryStream BufferStream;
        public readonly BinaryWriter BufferWriter;
        public readonly FileStream FileStream;
        public int LastAppendDataLength;

        public WriterWorkItem(FileStream fileStream)
        {
            FileStream = fileStream;
            BufferStream = new MemoryStream(8192);
            BufferWriter = new BinaryWriter(BufferStream);
        }

        public void AppendData(byte[] buf, int offset, int len)
        {
            LastAppendDataLength = len;
            FileStream.Write(buf, 0, len);
        }
        public void FlushToDisk()
        {
           FileStream.Flush(true);
        }
        public void ResizeStream(long length)
        {
            FileStream.SetLength(length);
        }
        public void Dispose()
        {
            FileStream.Dispose();
        }
    }
}
