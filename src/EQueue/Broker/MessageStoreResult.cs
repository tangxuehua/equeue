using EQueue.Broker.Storage;

namespace EQueue.Broker
{
    public class MessageStoreResult
    {
        public MessageLogRecord MessageLogRecord { get; private set; }

        public MessageStoreResult(MessageLogRecord messageLogRecord)
        {
            MessageLogRecord = messageLogRecord;
        }
    }
}
