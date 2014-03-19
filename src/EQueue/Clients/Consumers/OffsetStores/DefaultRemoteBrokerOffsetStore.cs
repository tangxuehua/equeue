using System;
using System.Collections.Concurrent;
using ECommon.IoC;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers.OffsetStores
{
    public class DefaultRemoteBrokerOffsetStore : IRemoteBrokerOffsetStore
    {
        private readonly SocketRemotingClient _remotingClient;
        private readonly IBinarySerializer _binarySerializer;
        private readonly ILogger _logger;
        private ConcurrentDictionary<string, ConcurrentDictionary<string, long>> _dict = new ConcurrentDictionary<string, ConcurrentDictionary<string, long>>();

        public DefaultRemoteBrokerOffsetStore(string brokerAddress, int brokerPort)
        {
            _remotingClient = new SocketRemotingClient(brokerAddress, brokerPort);
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public void Start()
        {
            _remotingClient.Start();
        }
        public void UpdateOffset(string groupName, MessageQueue messageQueue, long queueOffset)
        {
            var queueOffsetDict = _dict.GetOrAdd(groupName, new ConcurrentDictionary<string, long>());
            var key = string.Format("{0}-{1}", messageQueue.Topic, messageQueue.QueueId);
            queueOffsetDict.AddOrUpdate(key, queueOffset, (currentKey, oldOffset) =>
            {
                return queueOffset > oldOffset ? queueOffset : oldOffset;
            });
        }

        public void PersistOffset(string groupName, MessageQueue messageQueue)
        {
            ConcurrentDictionary<string, long> queueOffsetDict;
            if (_dict.TryGetValue(groupName, out queueOffsetDict))
            {
                var key = string.Format("{0}-{1}", messageQueue.Topic, messageQueue.QueueId);
                long queueOffset;
                if (queueOffsetDict.TryGetValue(key, out queueOffset))
                {
                    var request = new UpdateQueueOffsetRequest(groupName, messageQueue, queueOffset);
                    var remotingRequest = new RemotingRequest((int)RequestCode.UpdateQueueOffsetRequest, _binarySerializer.Serialize(request));
                    try
                    {
                        _remotingClient.InvokeOneway(remotingRequest, 10000);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error("Send UpdateQueueOffsetRequest has exception.", ex);
                    }
                }
            }
        }
    }
}
