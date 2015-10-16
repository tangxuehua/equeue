using System;
using System.IO;

namespace EQueue.Broker.Storage.LogRecords
{
    public class BufferLogRecord : ILogRecord
    {
        public byte[] RecordBuffer { get; set; }

        public void WriteTo(long logPosition, BinaryWriter writer)
        {
            writer.Write(RecordBuffer);
        }

        public void ReadFrom(int length, BinaryReader reader)
        {
            RecordBuffer = new byte[4 + length];
            Buffer.BlockCopy(BitConverter.GetBytes(length), 0, RecordBuffer, 0, 4);
            reader.Read(RecordBuffer, 4, length);
        }
    }
}
