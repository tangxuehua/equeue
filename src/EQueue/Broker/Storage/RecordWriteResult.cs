using System;

namespace EQueue.Broker.Storage
{
    public struct RecordWriteResult
    {
        public readonly bool Success;
        public readonly long OldPosition;
        public readonly long NewPosition;

        public RecordWriteResult(bool success, long oldPosition, long newPosition)
        {
            if (newPosition < oldPosition)
                throw new ArgumentException(string.Format("New position [{0}] is less than old position [{1}].", newPosition, oldPosition));

            Success = success;
            OldPosition = oldPosition;
            NewPosition = newPosition;
        }

        public static RecordWriteResult Failed(long position)
        {
            return new RecordWriteResult(false, position, position);
        }

        public static RecordWriteResult Successful(long oldPosition, long newPosition)
        {
            return new RecordWriteResult(true, oldPosition, newPosition);
        }

        public override string ToString()
        {
            return string.Format("[Success:{0}, OldPosition:{1}, NewPosition:{2}]", Success, OldPosition, NewPosition);
        }
    }
}
