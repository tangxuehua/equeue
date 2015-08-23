using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public class SendResult
    {
        public SendStatus SendStatus { get; private set; }
        public SendMessageResponse MessageResult { get; private set; }
        public string ErrorMessage { get; private set; }

        public SendResult(SendStatus sendStatus, SendMessageResponse messageResult, string errorMessage)
        {
            SendStatus = sendStatus;
            MessageResult = messageResult;
            ErrorMessage = errorMessage;
        }

        public override string ToString()
        {
            return string.Format("[SendStatus:{0},MessageResult:{1},ErrorMessage:{2}]", SendStatus, MessageResult, ErrorMessage);
        }
    }
}
