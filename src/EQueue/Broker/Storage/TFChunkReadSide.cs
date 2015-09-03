using System;
using System.IO;
using ECommon.Utilities;

namespace EQueue.Broker.Storage
{
    public partial class TFChunk
    {
        public interface IChunkReadSide
        {
            bool ExistsAt(long dataPosition);
            RecordReadResult TryReadAt(long dataPosition);
            RecordReadResult TryReadFirst();
            RecordReadResult TryReadClosestForward(long dataPosition);
            RecordReadResult TryReadLast();
            RecordReadResult TryReadClosestBackward(long dataPosition);
        }

        private class TFChunkReadSideUnscavenged : TFChunkReadSide, IChunkReadSide
        {
            public TFChunkReadSideUnscavenged(TFChunk chunk) : base(chunk) { }

            public bool ExistsAt(long dataPosition)
            {
                return dataPosition >= 0 && dataPosition < Chunk.DataPosition;
            }

            public RecordReadResult TryReadAt(long dataPosition)
            {
                var workItem = Chunk.GetReaderWorkItem();
                try
                {
                    if (dataPosition >= Chunk.DataPosition)
                    {
                        return RecordReadResult.Failure;
                    }

                    int length;
                    ILogRecord record;
                    var result = TryReadForwardInternal(workItem, dataPosition, out length, out record);
                    return new RecordReadResult(result, -1, record, length);
                }
                finally
                {
                    Chunk.ReturnReaderWorkItem(workItem);
                }
            }

            public RecordReadResult TryReadFirst()
            {
                return TryReadClosestForward(0);
            }

            public RecordReadResult TryReadClosestForward(long dataPosition)
            {
                var workItem = Chunk.GetReaderWorkItem();
                try
                {
                    if (dataPosition >= Chunk.DataPosition)
                    {
                        return RecordReadResult.Failure;
                    }

                    int length;
                    ILogRecord record;
                    if (!TryReadForwardInternal(workItem, dataPosition, out length, out record))
                        return RecordReadResult.Failure;

                    long nextLogicalPos = dataPosition + length + 2 * sizeof(int);
                    return new RecordReadResult(true, nextLogicalPos, record, length);
                }
                finally
                {
                    Chunk.ReturnReaderWorkItem(workItem);
                }
            }

            public RecordReadResult TryReadLast()
            {
                return TryReadClosestBackward(Chunk.DataPosition);
            }

            public RecordReadResult TryReadClosestBackward(long dataPosition)
            {
                var workItem = Chunk.GetReaderWorkItem();
                try
                {
                    // here we allow actualPosition == _logicalDataSize as we can read backward the very last record that way
                    if (dataPosition > Chunk.DataPosition)
                        return RecordReadResult.Failure;

                    int length;
                    ILogRecord record;
                    if (!TryReadBackwardInternal(workItem, dataPosition, out length, out record))
                        return RecordReadResult.Failure;

                    long nextLogicalPos = dataPosition - length - 2 * sizeof(int);
                    return new RecordReadResult(true, nextLogicalPos, record, length);
                }
                finally
                {
                    Chunk.ReturnReaderWorkItem(workItem);
                }
            }
        }

        private abstract class TFChunkReadSide
        {
            protected readonly TFChunk Chunk;

            protected TFChunkReadSide(TFChunk chunk)
            {
                Ensure.NotNull(chunk, "chunk");
                Chunk = chunk;
            }

            protected bool TryReadForwardInternal(ReaderWorkItem workItem, long dataPosition, out int length, out ILogRecord record)
            {
                length = -1;
                record = null;

                workItem.Stream.Position = GetStreamPosition(dataPosition);

                if (dataPosition + 2 * sizeof(int) > Chunk.DataPosition) // no space even for length prefix and suffix
                    return false;

                length = workItem.Reader.ReadInt32();
                if (length <= 0)
                {
                    throw new InvalidReadException(
                        string.Format("Log record at actual pos {0} has non-positive length: {1}. "
                                      + " in chunk.", dataPosition, length, Chunk));
                }
                if (length > Consts.MaxLogRecordSize)
                {
                    throw new InvalidReadException(
                        string.Format("Log record at actual pos {0} has too large length: {1} bytes, "
                                      + "while limit is {2} bytes. In chunk {3}.",
                                      dataPosition, length, Consts.MaxLogRecordSize, Chunk));
                }
                if (dataPosition + length + 2 * sizeof(int) > Chunk.DataPosition)
                {
                    throw new UnableToReadPastEndOfStreamException(
                        string.Format("There is not enough space to read full record (length prefix: {0}). "
                                      + "Actual pre-position: {1}. Something is seriously wrong in chunk {2}.",
                                      length, dataPosition, Chunk));
                }

                var recordType = workItem.Reader.ReadByte();
                var recordParser = _logRecordParserProvider.GetLogRecordParser(recordType);
                record = recordParser.ParseFrom(workItem.Reader);

                // verify suffix length == prefix length
                int suffixLength = workItem.Reader.ReadInt32();
                if (suffixLength != length)
                {
                    throw new Exception(
                        string.Format("Prefix/suffix length inconsistency: prefix length({0}) != suffix length ({1}).\n"
                                      + "Actual pre-position: {2}. Something is seriously wrong in chunk {3}.",
                                      length, suffixLength, dataPosition, Chunk));
                }

                return true;
            }

            protected bool TryReadBackwardInternal(ReaderWorkItem workItem, long dataPosition, out int length, out ILogRecord record)
            {
                length = -1;
                record = null;

                if (dataPosition < 2 * sizeof(int)) // no space even for length prefix and suffix 
                    return false;

                var realPos = GetStreamPosition(dataPosition);
                workItem.Stream.Position = realPos - sizeof(int);

                length = workItem.Reader.ReadInt32();
                if (length <= 0)
                {
                    throw new InvalidReadException(
                        string.Format("Log record that ends at actual pos {0} has non-positive length: {1}. "
                                      + "In chunk {2}.",
                                      dataPosition, length, Chunk));
                }
                if (length > Consts.MaxLogRecordSize)
                {
                    throw new ArgumentException(
                        string.Format("Log record that ends at actual pos {0} has too large length: {1} bytes, "
                                      + "while limit is {2} bytes. In chunk {3}.",
                                      dataPosition, length, Consts.MaxLogRecordSize, Chunk));
                }
                if (dataPosition < length + 2 * sizeof(int)) // no space for record + length prefix and suffix 
                {
                    throw new UnableToReadPastEndOfStreamException(
                        string.Format("There is not enough space to read full record (length suffix: {0}). "
                                      + "Actual post-position: {1}. Something is seriously wrong in chunk {2}.",
                                      length, dataPosition, Chunk));
                }

                workItem.Stream.Position = realPos - length - 2 * sizeof(int);

                // verify suffix length == prefix length
                int prefixLength = workItem.Reader.ReadInt32();
                if (prefixLength != length)
                {
                    throw new Exception(
                            string.Format("Prefix/suffix length inconsistency: prefix length({0}) != suffix length ({1})"
                                          + "Actual post-position: {2}. Something is seriously wrong in chunk {3}.",
                                          prefixLength, length, dataPosition, Chunk));
                }
                var recordType = workItem.Reader.ReadByte();
                var recordParser = _logRecordParserProvider.GetLogRecordParser(recordType);
                record = recordParser.ParseFrom(workItem.Reader);

                return true;
            }
        }
    }
}
