using System.Collections.Concurrent;
using System.Collections.Generic;
using EQueue.Common;

namespace EQueue.Broker
{
    public class MessageService : IMessageService
    {
        private const int DefaultConsumeQueueCount = 4;
        private ConcurrentDictionary<string, long> _queueCurrentOffsetDict = new ConcurrentDictionary<string, long>();
        private ConcurrentDictionary<string, IList<ConsumeQueue>> _consumeQueueDict = new ConcurrentDictionary<string, IList<ConsumeQueue>>();
        private IMessageQueueSelector _messageQueueSelector;
        private IMessageStore _messageStore;

        public MessageService(IMessageQueueSelector messageQueueSelector, IMessageStore messageStore)
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
