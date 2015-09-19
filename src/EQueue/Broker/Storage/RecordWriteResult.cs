using System;

namespace EQueue.Broker.Storage
{
    public enum RecordWriteResultStatus
    {
        /// <summary>写入成功
        /// </summary>
        Success,
        /// <summary>剩余空间不足
        /// </summary>
        NotEnoughSpace
    }
    public struct RecordWriteResult
    {
        public readonly RecordWriteResultStatus Status;
        public readonly long OldPosition;
        public readonly long NewPosition;

        public RecordWriteResult(RecordWriteResultStatus status, long oldPosition, long newPosition)
        {
            if (newPosition < oldPosition)
            {
                throw new ArgumentException(string.Format("New position [{0}] is less than old position [{1}].", newPosition, oldPosition));
            }

            Status = status;
            OldPosition = oldPosition;
            NewPosition = newPosition;
        }

        public static RecordWriteResult NotEnoughSpace(long position)
        {
            return new RecordWriteResult(RecordWriteResultStatus.NotEnoughSpace, position, position);
        }
        public static RecordWriteResult Successful(long oldPosition, long newPosition)
        {
            return new RecordWriteResult(RecordWriteResultStatus.Success, oldPosition, newPosition);
        }

        public override string ToString()
        {
            return string.Format("[Status:{0}, OldPosition:{1}, NewPosition:{2}]", Status, OldPosition, NewPosition);
        }
    }
}
