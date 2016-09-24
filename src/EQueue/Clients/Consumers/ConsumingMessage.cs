using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class ConsumingMessage
    {
        public QueueMessage Message { get; private set; }
        public PullRequest PullRequest { get; private set; }
        public bool IsIgnored { get; set; }

        public ConsumingMessage(QueueMessage message, PullRequest pullRequest)
        {
            Message = message;
            PullRequest = pullRequest;
            Message.BrokerName = pullRequest.MessageQueue.BrokerName;
        }
    }
}
