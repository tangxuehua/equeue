using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ECommon.IoC;
using ECommon.Logging;
using ECommon.Scheduling;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class MessageService : IMessageService
    {
        private ConcurrentDictionary<string, long> _queueCurrentOffsetDict = new ConcurrentDictionary<string, long>();
        private ConcurrentDictionary<string, IList<Queue>> _topicQueueDict = new ConcurrentDictionary<string, IList<Queue>>();
        private readonly IMessageStore _messageStore;
        private readonly IOffsetManager _offsetManager;
        private readonly IScheduleService _scheduleService;
        private ILogger _logger;
        private BrokerController _brokerController;
        private int _deleteConsumedMessageTaskId;

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
            _messageStore.Recover();
            RecoverTopicQueues();
            _messageStore.Start();
            _deleteConsumedMessageTaskId = _scheduleService.ScheduleTask(
                DeleteConsumedMessage,
                _brokerController.Setting.DeleteMessageInterval,
                _brokerController.Setting.DeleteMessageInterval);
        }
        public void Shutdown()
        {
            _messageStore.Shutdown();
            _scheduleService.ShutdownTask(_deleteConsumedMessageTaskId);
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
            queue.AddQueueItem(queueMessage);
            return new MessageStoreResult(queueMessage.MessageOffset, queue.QueueId, queueOffset);
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
                    var queueItem = queue.GetQueueItem(currentQueueOffset);
                    if (queueItem != null)
                    {
                        var message = _messageStore.GetMessage(queueItem.MessageOffset);
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

        private void RecoverTopicQueues()
        {
            foreach (var message in _messageStore.Messages)
            {
                var queues = GetQueues(message.Topic);
                if (message.QueueId >= queues.Count)
                {
                    for (var index = queues.Count; index <= message.QueueId; index++)
                    {
                        queues.Add(new Queue(message.Topic, index));
                    }
                }
                var queue = queues[message.QueueId];
                queue.RecoverQueueItem(message);
            }
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
        private void DeleteConsumedMessage()
        {
            foreach (var topicQueues in _topicQueueDict.Values)
            {
                foreach (var queue in topicQueues)
                {
                    var consumedOffset = _offsetManager.GetMinOffset(queue.Topic, queue.QueueId);
                    for (var index = queue.MaxRemovedOffset + 1; index <= consumedOffset; index++)
                    {
                        var queueItem = queue.RemoveQueueItem(index);
                        if (queueItem != null)
                        {
                            _messageStore.RemoveMessage(queueItem.MessageOffset);
                        }
                    }
                    var maxQueueOffset = consumedOffset;
                    if (maxQueueOffset >= queue.CurrentOffset)
                    {
                        maxQueueOffset = queue.CurrentOffset;
                    }
                    _messageStore.DeleteMessages(queue.Topic, queue.QueueId, maxQueueOffset);
                }
            }
        }
    }
}
