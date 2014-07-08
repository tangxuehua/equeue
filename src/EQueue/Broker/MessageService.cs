using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class MessageService : IMessageService
    {
        private ConcurrentDictionary<string, IList<Queue>> _topicQueueDict = new ConcurrentDictionary<string, IList<Queue>>();
        private readonly IMessageStore _messageStore;
        private readonly IOffsetManager _offsetManager;
        private readonly IScheduleService _scheduleService;
        private ILogger _logger;
        private BrokerController _brokerController;
        private int _removeConsumedMessagesTaskId;

        public MessageService(IMessageStore messageStore, IOffsetManager offsetManager, IScheduleService scheduleService)
        {
            _messageStore = messageStore;
            _offsetManager = offsetManager;
            _scheduleService = scheduleService;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void SetBrokerContrller(BrokerController brokerController)
        {
            _brokerController = brokerController;
        }
        public void Start()
        {
            Clear();
            _messageStore.Recover(RecoverQueueIndexForMessage);
            _offsetManager.Recover();
            _messageStore.Start();
            _offsetManager.Start();
            _removeConsumedMessagesTaskId = _scheduleService.ScheduleTask("MessageService.RemoveConsumedMessages", RemoveConsumedMessages, _brokerController.Setting.RemoveMessageInterval, _brokerController.Setting.RemoveMessageInterval);
        }
        public void Shutdown()
        {
            _messageStore.Shutdown();
            _offsetManager.Shutdown();
            _scheduleService.ShutdownTask(_removeConsumedMessagesTaskId);
        }
        public MessageStoreResult StoreMessage(Message message, int queueId)
        {
            var queues = GetQueues(message.Topic);
            var queueCount = queues.Count;
            if (queueId >= queueCount || queueId < 0)
            {
                throw new InvalidQueueIdException(message.Topic, queueCount, queueId);
            }
            var queue = queues[queueId];
            var queueOffset = queue.IncrementCurrentOffset();
            var queueMessage = _messageStore.StoreMessage(queueId, queueOffset, message);
            queue.AddQueueItem(queueMessage.QueueOffset, queueMessage.MessageOffset);
            return new MessageStoreResult(queueMessage.MessageOffset, queueMessage.QueueId, queueMessage.QueueOffset);
        }
        public IEnumerable<QueueMessage> GetMessages(string topic, int queueId, long queueOffset, int batchSize)
        {
            var queues = GetQueues(topic);
            var queue = queues.SingleOrDefault(x => x.QueueId == queueId);
            if (queue != null)
            {
                var currentQueueOffset = queueOffset;
                var maxQueueOffset = queueOffset + batchSize;
                var messages = new List<QueueMessage>();
                while (currentQueueOffset < maxQueueOffset)
                {
                    var messageOffset = queue.GetMessageOffset(currentQueueOffset);
                    if (messageOffset >= 0)
                    {
                        var message = _messageStore.GetMessage(messageOffset);
                        if (message != null)
                        {
                            messages.Add(message);
                        }
                    }
                    currentQueueOffset++;
                }
                return messages;
            }
            return new QueueMessage[0];
        }
        public long GetQueueCurrentOffset(string topic, int queueId)
        {
            var queues = GetQueues(topic);
            var queue = queues.SingleOrDefault(x => x.QueueId == queueId);
            if (queue != null)
            {
                return queue.CurrentOffset;
            }
            return -1;
        }
        public long GetQueueMinOffset(string topic, int queueId)
        {
            var queues = GetQueues(topic);
            var queue = queues.SingleOrDefault(x => x.QueueId == queueId);
            if (queue != null)
            {
                var offset = queue.GetMinQueueOffset();
                if (offset != null)
                {
                    return offset.Value;
                }
            }
            return -1;
        }
        public int GetTopicQueueCount(string topic)
        {
            return GetQueues(topic).Count;
        }

        private void Clear()
        {
            _topicQueueDict.Clear();
        }
        private void RecoverQueueIndexForMessage(long messageOffset, string topic, int queueId, long queueOffset)
        {
            var queues = GetQueues(topic);
            if (queueId >= queues.Count)
            {
                for (var index = queues.Count; index <= queueId; index++)
                {
                    queues.Add(new Queue(topic, index));
                }
            }
            var queue = queues[queueId];
            queue.RecoverQueueItem(queueOffset, messageOffset);
        }
        private IList<Queue> GetQueues(string topic)
        {
            return _topicQueueDict.GetOrAdd(topic, x =>
            {
                var queues = new List<Queue>();
                for (var index = 0; index < _brokerController.Setting.DefaultTopicQueueCount; index++)
                {
                    queues.Add(new Queue(x, index));
                }
                return queues;
            });
        }
        private void RemoveConsumedMessages()
        {
            foreach (var topicQueues in _topicQueueDict.Values)
            {
                foreach (var queue in topicQueues)
                {
                    var consumedQueueOffset = _offsetManager.GetMinOffset(queue.Topic, queue.QueueId);
                    if (consumedQueueOffset > queue.CurrentOffset)
                    {
                        consumedQueueOffset = queue.CurrentOffset;
                    }
                    queue.RemoveQueueItems(consumedQueueOffset);
                    _messageStore.UpdateMaxAllowToDeleteQueueOffset(queue.Topic, queue.QueueId, consumedQueueOffset);
                }
            }
        }
    }
}
