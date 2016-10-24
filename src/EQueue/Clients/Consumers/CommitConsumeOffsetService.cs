using System;
using System.Collections.Concurrent;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using ECommon.Utilities;
using EQueue.Protocols;
using EQueue.Protocols.Brokers;
using EQueue.Protocols.Brokers.Requests;

namespace EQueue.Clients.Consumers
{
    public class CommitConsumeOffsetService
    {
        #region Private Variables

        private readonly string _clientId;
        private readonly Consumer _consumer;
        private readonly ClientService _clientService;
        private readonly IBinarySerializer _binarySerializer;
        private readonly ILogger _logger;
        private readonly IScheduleService _scheduleService;
        private readonly ConcurrentDictionary<string, ConsumeOffsetInfo> _consumeOffsetInfoDict;

        #endregion

        public CommitConsumeOffsetService(Consumer consumer, ClientService clientService)
        {
            _consumeOffsetInfoDict = new ConcurrentDictionary<string, ConsumeOffsetInfo>();
            _consumer = consumer;
            _clientService = clientService;
            _clientId = clientService.GetClientId();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Start()
        {
            if (_consumer.Setting.CommitConsumeOffsetAsync)
            {
                _scheduleService.StartTask("CommitOffsets", CommitOffsets, 1000, _consumer.Setting.CommitConsumerOffsetInterval);
            }
            _logger.InfoFormat("{0} startted.", GetType().Name);
        }
        public void Stop()
        {
            if (_consumer.Setting.CommitConsumeOffsetAsync)
            {
                _scheduleService.StopTask("CommitOffsets");
            }
            _logger.InfoFormat("{0} stopped.", GetType().Name);
        }
        public void CommitConsumeOffset(string brokerName, string topic, int queueId, long consumeOffset)
        {
            Ensure.NotNullOrEmpty(brokerName, "brokerName");
            Ensure.NotNullOrEmpty(topic, "topic");
            Ensure.Nonnegative(queueId, "queueId");
            Ensure.Nonnegative(consumeOffset, "consumeOffset");

            if (_consumer.Setting.CommitConsumeOffsetAsync)
            {
                var key = string.Format("{0}_{1}_{2}", brokerName, topic, queueId);
                _consumeOffsetInfoDict.AddOrUpdate(key, x =>
                {
                    return new ConsumeOffsetInfo { MessageQueue = new MessageQueue(brokerName, topic, queueId), ConsumeOffset = consumeOffset };
                },
                (x, y) =>
                {
                    y.ConsumeOffset = consumeOffset;
                    return y;
                });
            }
            else
            {
                CommitConsumeOffset(new MessageQueue(brokerName, topic, queueId), consumeOffset, true);
            }
        }
        public void CommitConsumeOffset(PullRequest pullRequest)
        {
            var consumedOffset = pullRequest.ProcessQueue.GetConsumedQueueOffset();
            if (consumedOffset >= 0)
            {
                if (!pullRequest.ProcessQueue.TryUpdatePreviousConsumedQueueOffset(consumedOffset))
                {
                    return;
                }
                CommitConsumeOffset(pullRequest.MessageQueue, consumedOffset);
            }
        }
        public void CommitConsumeOffset(MessageQueue messageQueue, long consumeOffset, bool throwIfException = false)
        {
            Ensure.NotNull(messageQueue, "messageQueue");
            Ensure.Nonnegative(consumeOffset, "consumeOffset");

            var brokerConnection = _clientService.GetBrokerConnection(messageQueue.BrokerName);
            if (brokerConnection == null)
            {
                _logger.ErrorFormat("CommitConsumeOffset failed as the target broker connection not found, messageQueue:{0}", messageQueue);
                return;
            }
            var remotingClient = brokerConnection.AdminRemotingClient;

            var request = new UpdateQueueOffsetRequest(_consumer.GroupName, messageQueue, consumeOffset);
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.UpdateQueueConsumeOffsetRequest, _binarySerializer.Serialize(request));
            var brokerAddress = remotingClient.ServerEndPoint.ToAddress();

            try
            {
                remotingClient.InvokeOneway(remotingRequest);
                if (_logger.IsDebugEnabled)
                {
                    _logger.DebugFormat("CommitConsumeOffset success, consumerGroup:{0}, consumerId:{1}, messageQueue:{2}, consumeOffset:{3}, brokerAddress:{4}",
                        _consumer.GroupName,
                        _clientId,
                        messageQueue,
                        consumeOffset,
                        brokerAddress);
                }
            }
            catch (Exception ex)
            {
                if (remotingClient.IsConnected)
                {
                    _logger.Error(string.Format("CommitConsumeOffset has exception, consumerGroup:{0}, consumerId:{1}, messageQueue:{2}, consumeOffset:{3}, brokerAddress:{4}",
                        _consumer.GroupName,
                        _clientId,
                        messageQueue,
                        consumeOffset,
                        brokerAddress), ex);
                }
                if (throwIfException)
                {
                    throw;
                }
            }
        }

        private void CommitOffsets()
        {
            foreach (var consumeOffsetInfo in _consumeOffsetInfoDict.Values)
            {
                CommitConsumeOffset(consumeOffsetInfo.MessageQueue, consumeOffsetInfo.ConsumeOffset); ;
            }
        }
        class ConsumeOffsetInfo
        {
            public MessageQueue MessageQueue;
            public long ConsumeOffset;
        }
    }
}
