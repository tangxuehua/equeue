using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using EQueue.Common;

namespace EQueue.Broker
{
    public class MessageService : IMessageService
    {
        private const int DefaultConsumeQueueCount = 4;
        private ConcurrentDictionary<string, long> _queueCurrentOffsetDict = new ConcurrentDictionary<string, long>();
        private ConcurrentDictionary<string, IList<ConsumeQueue>> _consumeQueueDict = new ConcurrentDictionary<string, IList<ConsumeQueue>>();
        private IConsumeQueueSelector _messageQueueSelector;
        private IMessageStore _messageStore;

        public MessageService(IConsumeQueueSelector messageQueueSelector, IMessageStore messageStore)
        {
            _messageQueueSelector = messageQueueSelector;
            _messageStore = messageStore;
        }

        public MessageStoreResult StoreMessage(Message message, object arg)
        {
            var consumeQueues = GetConsumeQueues(message.Topic);
            var consumeQueue = _messageQueueSelector.SelectQueue(consumeQueues, message, arg);
            var queueOffset = consumeQueue.GetNextOffset();
            return _messageStore.StoreMessage(message, consumeQueue.QueueId, queueOffset);
        }
        public QueueMessage GetMessage(string topic, int queueId, long queueOffset)
        {
            var consumeQueues = GetConsumeQueues(topic);
            var consumeQueue = consumeQueues.SingleOrDefault(x => x.QueueId == queueId);
            if (consumeQueue != null)
            {
                var messageOffset = consumeQueue.GetMessageOffset(queueOffset);
                if (messageOffset >= 0)
                {
                    return _messageStore.GetMessage(messageOffset);
                }
            }
            return null;
        }
        public long GetQueueCurrentOffset(string topic, int queueId)
        {
            var consumeQueues = GetConsumeQueues(topic);
            var consumeQueue = consumeQueues.SingleOrDefault(x => x.QueueId == queueId);
            if (consumeQueue != null)
            {
                return consumeQueue.CurrentOffset;
            }
            return -1;
        }

        private IList<ConsumeQueue> GetConsumeQueues(string topic)
        {
            return _consumeQueueDict.GetOrAdd(topic, (x) =>
            {
                var queues = new List<ConsumeQueue>();
                for (var index = 0; index < DefaultConsumeQueueCount; index++)
                {
                    queues.Add(new ConsumeQueue(x, index));
                }
                return queues;
            });
        }
    }
}
