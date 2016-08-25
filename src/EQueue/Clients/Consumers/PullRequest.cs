using System;
using System.Collections.Generic;
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
        public DateTime PullStartTime { get; set; }
        public HashSet<string> Tags { get; set; }
        public bool IsDropped { get; set; }

        public PullRequest(string consumerId, string groupName, MessageQueue messageQueue, long nextConsumeOffset, HashSet<string> tags)
        {
            ConsumerId = consumerId;
            GroupName = groupName;
            MessageQueue = messageQueue;
            NextConsumeOffset = nextConsumeOffset;
            ProcessQueue = new ProcessQueue();
            Tags = tags ?? new HashSet<string>();
        }

        public override string ToString()
        {
            return string.Format("[ConsumerId={0}, Group={1}, MessageQueue={2}, NextConsumeOffset={3}, Tags={4}, IsDropped={5}]", ConsumerId, GroupName, MessageQueue, NextConsumeOffset, string.Join("|", Tags), IsDropped);
        }
    }
}
