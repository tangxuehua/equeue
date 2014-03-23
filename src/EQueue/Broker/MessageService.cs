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
        private const int DefaultTopicQueueCount = 4;
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
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public void SetBrokerContrller(BrokerController brokerController)
        {
            _brokerController = brokerController;
        }
        public void Start()
        {
            _deleteConsumedMessageTaskId = _scheduleService.ScheduleTask(
                DeleteConsumedMessage,
                _brokerController.Setting.DeleteMessageInterval,
                _brokerController.Setting.DeleteMessageInterval);
        }
        public void Shutdown()
        {
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
            var queueMessage = _messageStore.StoreMessage(message, queue.QueueId, queueOffset);
            queue.SetQueueMessage(queueOffset, queueMessage);
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
                    var queueItem = queue.GetQueueItem(currentQueueOffset);
                    if (queueItem != null)
                    {
                        var message = _messageStore.GetMessage(queueItem.MessageOffset);
                        if (message != null)
                        {
                            messages.Add(message);
                        }
                    }
                    else
                    {
                        break;
                    }
                    currentQueueOffset++;
                }
                return messages;
            }
            return new QueueMessage[0];
        }
        public long GetQueueOffset(string topic, int queueId)
        {
            var queues = GetQueues(topic);
            var queue = queues.SingleOrDefault(x => x.QueueId == queueId);
            if (queue != null)
            {
                return queue.CurrentOffset;
            }
            return -1;
        }
        public int GetTopicQueueCount(string topic)
        {
            return GetQueues(topic).Count;
        }

        private IList<Queue> GetQueues(string topic)
        {
            return _topicQueueDict.GetOrAdd(topic, x =>
            {
                var queues = new List<Queue>();
                for (var index = 0; index < DefaultTopicQueueCount; index++)
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
                    var deletedMessageCount = 0;
                    var consumedOffset = _offsetManager.GetMinOffset(queue.Topic, queue.QueueId);
                    for (var index = queue.MaxRemovedOffset + 1; index <= consumedOffset; index++)
                    {
                        var queueItem = queue.RemoveQueueItem(index);
                        if (queueItem != null)
                        {
                            if (_messageStore.RemoveMessage(queueItem.MessageOffset))
                            {
                                deletedMessageCount++;
                            }
                        }
                    }
                    if (deletedMessageCount > 0)
                    {
                        _logger.DebugFormat("Deleted {0} messages for Queue{1} of Topic [{2}].", deletedMessageCount, queue.QueueId, queue.Topic);
                    }
                }
            }
        }
    }
}
