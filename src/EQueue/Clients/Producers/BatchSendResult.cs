using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public class BatchSendResult
    {
        public SendStatus SendStatus { get; private set; }
        public BatchMessageStoreResult MessageStoreResult { get; private set; }
        public string ErrorMessage { get; private set; }

        public BatchSendResult(SendStatus sendStatus, BatchMessageStoreResult messageStoreResult, string errorMessage)
        {
            SendStatus = sendStatus;
            MessageStoreResult = messageStoreResult;
            ErrorMessage = errorMessage;
        }

        public override string ToString()
        {
            return string.Format("[SendStatus:{0},MessageStoreResult:{1},ErrorMessage:{2}]", SendStatus, MessageStoreResult, ErrorMessage);
        }
    }
}
