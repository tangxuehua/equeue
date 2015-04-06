using System;
using System.Collections.Generic;
using ECommon.Components;
using ECommon.Logging;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class MessageService : IMessageService
    {
        private readonly IQueueService _queueService;
        private readonly IMessageStore _messageStore;
        private readonly ILogger _logger;
        private long _totalRecoveredQueueIndex;

        public MessageService(IQueueService queueService, IMessageStore messageStore, IOffsetManager offsetManager)
        {
            _queueService = queueService;
            _messageStore = messageStore;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Start()
        {
            _totalRecoveredQueueIndex = 0;
            _messageStore.Recover(ProcessRecoveredMessage);
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
                throw new Exception(string.Format("No available queue for topic: {0}, queueId: {1}", message.Topic, queueId));
            }
            var queueOffset = queue.IncrementCurrentOffset();
            var queueMessage = _messageStore.StoreMessage(queueId, queueOffset, message, routingKey);
            queue.SetQueueIndex(queueMessage.QueueOffset, queueMessage.MessageOffset);
            return new MessageStoreResult(queueMessage.MessageId, queueMessage.MessageOffset, queueMessage.QueueId, queueMessage.QueueOffset);
        }
        public IEnumerable<QueueMessage> GetMessages(string topic, int queueId, long queueOffset, int batchSize)
        {
            var queue = _queueService.GetQueue(topic, queueId);
            if (queue != null)
            {
                var messages = new List<QueueMessage>();
                var maxQueueOffset = queueOffset + batchSize > queue.CurrentOffset ? queue.CurrentOffset + 1 : queueOffset + batchSize;
                for (var currentQueueOffset = queueOffset; currentQueueOffset < maxQueueOffset; currentQueueOffset++)
                {
                    var messageOffset = queue.GetMessageOffset(currentQueueOffset);
                    if (messageOffset >= 0)
                    {
                        var message = _messageStore.GetMessage(messageOffset);
                        if (message != null)
                        {
                            messages.Add(message);
                        }
                        else
                        {
                            _logger.ErrorFormat("Cannot find the message by messageOffset, please check if the message exist. topic:{0}, queueId:{1}, queueOffset:{2}, messageOffset:{3}", topic, queueId, currentQueueOffset, messageOffset);
                        }
                    }
                    else
                    {
                        if (currentQueueOffset < queue.CurrentOffset && _messageStore.SupportBatchLoadQueueIndex)
                        {
                            //Batch load queue index from message store.
                            var indexDict = _messageStore.BatchLoadQueueIndex(topic, queueId, currentQueueOffset);
                            foreach (var entry in indexDict)
                            {
                                queue.SetQueueIndex(entry.Key, entry.Value);
                            }

                            //Get message offset again from queue.
                            messageOffset = queue.GetMessageOffset(currentQueueOffset);
                            if (messageOffset >= 0)
                            {
                                var message = _messageStore.GetMessage(messageOffset);
                                if (message != null)
                                {
                                    messages.Add(message);
                                }
                                else
                                {
                                    _logger.ErrorFormat("Cannot find the message by messageOffset after batch loading queue index, please check if the message exist. topic:{0}, queueId:{1}, queueOffset:{2}, messageOffset:{3}", topic, queueId, currentQueueOffset, messageOffset);
                                }
                            }
                            else
                            {
                                _logger.ErrorFormat("Cannot find the messageOffset by queueOffset, please check if the message exist. topic:{0}, queueId:{1}, queueOffset:{2}", topic, queueId, currentQueueOffset);
                            }
                        }
                    }
                }
                return messages;
            }
            return new QueueMessage[0];
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
