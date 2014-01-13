using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class PullRequest
    {
        public string ConsumerGroup { get; set; }
        public MessageQueue MessageQueue { get; set; }
        public ProcessQueue ProcessQueue { get; set; }
        public long NextOffset { get; set; }

        public override string ToString()
        {
            return string.Format("[ConsumerGroup={0}, MessageQueue={1}, NextOffset={2}]", ConsumerGroup, MessageQueue, NextOffset);
        }
    }
}
