using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public class SendResult
    {
        public SendStatus SendStatus { get; private set; }
        public string ErrorMessage { get; private set; }
        public MessageQueue MessageQueue { get; private set; }
        public long QueueOffset { get; private set; }
        public long MessageOffset { get; private set; }
        public string MessageId { get; private set; }

        public SendResult(SendStatus sendStatus, string errorMessage)
        {
            SendStatus = sendStatus;
            ErrorMessage = errorMessage;
        }
        public SendResult(SendStatus sendStatus, string messageId, long messageOffset, MessageQueue messageQueue, long queueOffset)
        {
            SendStatus = sendStatus;
            MessageId = messageId;
            MessageOffset = messageOffset;
            MessageQueue = messageQueue;
            QueueOffset = queueOffset;
        }

        public override string ToString()
        {
            return string.Format("[SendStatus={0}, MessageQueue={1}, QueueOffset={2}, MessageOffset={3}, MessageId={4}, ErrorMessage={5}]",
                SendStatus,
                MessageQueue,
                QueueOffset,
                MessageOffset,
                MessageId,
                ErrorMessage);
        }
    }
}
