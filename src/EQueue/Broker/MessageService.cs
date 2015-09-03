using System;
using System.Collections.Generic;
using ECommon.Components;
using ECommon.Logging;
using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class MessageService : IMessageService
    {
        private readonly IQueueService _queueService;
        private readonly IMessageStore _messageStore;
        private readonly IOffsetManager _offsetManager;
        private readonly ILogger _logger;
        private readonly object _syncObj = new object();
        private long _totalRecoveredQueueIndex;

        public MessageService(IQueueService queueService, IMessageStore messageStore, IOffsetManager offsetManager)
        {
            _queueService = queueService;
            _messageStore = messageStore;
            _offsetManager = offsetManager;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Start()
        {
            _totalRecoveredQueueIndex = 0;
            _messageStore.Recover(_offsetManager.GetQueueConsumedOffsets(), ProcessRecoveredMessage);
            _messageStore.Start();
        }
        public void Shutdown()
        {
            _messageStore.Shutdown();
        }
        public MessageStoreResult StoreMessage(Message message, int queueId, string routingKey)
        {
            var queue = _queueService.GetQueue(message.Topic, queueId);
            if (queue == null)
            {
                throw new Exception(string.Format("Queue not exist, topic: {0}, queueId: {1}", message.Topic, queueId));
            }
            lock (_syncObj)
            {
                var messageOffset = _messageStore.GetNextMessageOffset();
                var queueOffset = queue.IncrementCurrentOffset();
                var storeResult = _messageStore.StoreMessage(queueId, messageOffset, queueOffset, message, routingKey);
                //queue.SetQueueIndex(storeResult.MessageLogRecord.QueueOffset, storeResult.MessageLogRecord.LogPosition);
                return storeResult;
            }
        }
        public IEnumerable<MessageLogRecord> GetMessages(string topic, int queueId, long queueOffset, int batchSize)
        {
            var queue = _queueService.GetQueue(topic, queueId);
            if (queue == null)
            {
                return new MessageLogRecord[0];
            }

            var messages = new List<MessageLogRecord>();
            var currentQueueOffset = queueOffset;
            while (currentQueueOffset <= queue.CurrentOffset && messages.Count < batchSize)
            {
                var messageOffset = queue.GetMessageOffset(currentQueueOffset);
                if (messageOffset < 0)
                {
                    break;
                }
                if (messageOffset >= 0)
                {
                    var message = _messageStore.GetMessage(messageOffset);
                    if (message != null)
                    {
                        messages.Add(message);
                    }
                    else
                    {
                        queue.RemoveQueueOffset(currentQueueOffset);
                    }
                }
                currentQueueOffset++;
            }
            return messages;
        }

        private void BatchLoadQueueIndexToMemory(Queue queue, long startQueueOffset)
        {
            if (_messageStore.SupportBatchLoadQueueIndex)
            {
                var indexDict = _messageStore.BatchLoadQueueIndex(queue.Topic, queue.QueueId, startQueueOffset);
                foreach (var entry in indexDict)
                {
                    queue.SetQueueIndex(entry.Key, entry.Value);
                }
            }
        }
        private void ProcessRecoveredMessage(long messageOffset, string topic, int queueId, long queueOffset)
        {
            var queue = _queueService.GetQueue(topic, queueId);
            if (queue == null)
            {
                _logger.ErrorFormat("Queue not found when recovering message. messageOffset: {0}, topic: {1}, queueId: {2}, queueOffset: {3}", messageOffset, topic, queueId, queueOffset);
                return;
            }
            var allowSetQueueIndex = !_messageStore.SupportBatchLoadQueueIndex || _totalRecoveredQueueIndex < BrokerController.Instance.Setting.QueueIndexMaxCacheSize;
            queue.RecoverQueueIndex(queueOffset, messageOffset, allowSetQueueIndex);
            _totalRecoveredQueueIndex++;
        }
    }
}
