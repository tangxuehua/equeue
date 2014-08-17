using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class PullRequest
    {
        public string ConsumerId { get; private set; }
        public string GroupName { get; private set; }
        public MessageQueue MessageQueue { get; private set; }
        public ProcessQueue ProcessQueue { get; private set; }
        public long NextConsumeOffset { get; set; }

        public PullRequest(string consumerId, string groupName, MessageQueue messageQueue, long nextConsumeOffset)
        {
            ConsumerId = consumerId;
            GroupName = groupName;
            MessageQueue = messageQueue;
            NextConsumeOffset = nextConsumeOffset;
            ProcessQueue = new ProcessQueue();
        }

        public override string ToString()
        {
            return string.Format("[ConsumerId={0}, Group={1}, MessageQueue={2}, NextConsumeOffset={3}]", ConsumerId, GroupName, MessageQueue, NextConsumeOffset);
        }
    }
}
