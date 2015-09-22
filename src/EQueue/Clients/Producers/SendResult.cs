using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public class SendResult
    {
        public SendStatus SendStatus { get; private set; }
        public MessageStoreResult MessageStoreResult { get; private set; }
        public string ErrorMessage { get; private set; }

        public SendResult(SendStatus sendStatus, MessageStoreResult messageStoreResult, string errorMessage)
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
