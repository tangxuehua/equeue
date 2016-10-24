using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Socketing;

namespace EQueue.Broker.Client
{
    public class ProducerManager
    {
        class ProducerInfo
        {
            public string ProducerId;
            public ClientHeartbeatInfo HeartbeatInfo;
        }
        private readonly ConcurrentDictionary<string /*connectionId*/, ProducerInfo> _producerInfoDict = new ConcurrentDictionary<string, ProducerInfo>();
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;

        public ProducerManager()
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Start()
        {
            _producerInfoDict.Clear();
            _scheduleService.StartTask("ScanNotActiveProducer", ScanNotActiveProducer, 1000, 1000);
        }
        public void Shutdown()
        {
            _producerInfoDict.Clear();
            _scheduleService.StopTask("ScanNotActiveProducer");
        }
        public void RegisterProducer(ITcpConnection connection, string producerId)
        {
            var connectionId = connection.RemotingEndPoint.ToAddress();
            _producerInfoDict.AddOrUpdate(connectionId, key =>
            {
                var producerInfo = new ProducerInfo
                {
                    ProducerId = producerId,
                    HeartbeatInfo = new ClientHeartbeatInfo(connection) { LastHeartbeatTime = DateTime.Now }
                };
                _logger.InfoFormat("Producer registered, producerId: {0}, connectionId: {1}", producerId, key);
                return producerInfo;
            }, (key, existingProducerInfo) =>
            {
                existingProducerInfo.HeartbeatInfo.LastHeartbeatTime = DateTime.Now;
                return existingProducerInfo;
            });
        }
        public void RemoveProducer(string connectionId)
        {
            ProducerInfo producerInfo;
            if (_producerInfoDict.TryRemove(connectionId, out producerInfo))
            {
                try
                {
                    producerInfo.HeartbeatInfo.Connection.Close();
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("Close connection for producer failed, producerId: {0}, connectionId: {1}", producerInfo.ProducerId, connectionId), ex);
                }
                _logger.InfoFormat("Producer removed, producerId: {0}, connectionId: {1}, lastHeartbeat: {2}",
                    producerInfo.ProducerId,
                    connectionId,
                    producerInfo.HeartbeatInfo.LastHeartbeatTime);
            }
        }
        public int GetProducerCount()
        {
            return _producerInfoDict.Count;
        }
        public IEnumerable<string> GetAllProducers()
        {
            return _producerInfoDict.Values.Select(x => x.ProducerId).ToList();
        }
        public bool IsProducerExist(string producerId)
        {
            return _producerInfoDict.Values.Any(x => x.ProducerId == producerId);
        }

        private void ScanNotActiveProducer()
        {
            foreach (var entry in _producerInfoDict)
            {
                if (entry.Value.HeartbeatInfo.IsTimeout(BrokerController.Instance.Setting.ProducerExpiredTimeout))
                {
                    RemoveProducer(entry.Key);
                }
            }
        }
    }
}
