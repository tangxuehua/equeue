using EQueue.Broker.Storage;

namespace EQueue.Broker
{
    public class MessageStoreResult
    {
        public MessageLogRecord MessageLogRecord { get; private set; }
        public long NextLogPosition { get; private set; }

        public MessageStoreResult(MessageLogRecord messageLogRecord, long nextLogPosition)
        {
            MessageLogRecord = messageLogRecord;
            NextLogPosition = nextLogPosition;
        }
    }
}
