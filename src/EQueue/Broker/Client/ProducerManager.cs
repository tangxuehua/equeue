using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Socketing;

namespace EQueue.Broker.Client
{
    public class ProducerManager
    {
        class ProducerHeartbeatInfo
        {
            public ITcpConnection Connection { get; private set; }
            public DateTime LastHeartbeatTime { get; set; }

            public ProducerHeartbeatInfo(ITcpConnection connection)
            {
                Connection = connection;
            }
        }
        private readonly ConcurrentDictionary<string, ProducerHeartbeatInfo> _producerDict = new ConcurrentDictionary<string, ProducerHeartbeatInfo>();
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;

        public ProducerManager()
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Start()
        {
            _producerDict.Clear();
            _scheduleService.StartTask("ScanNotActiveProducer", ScanNotActiveProducer, 1000, 1000);
        }
        public void Shutdown()
        {
            _producerDict.Clear();
            _scheduleService.StopTask("ScanNotActiveProducer");
        }
        public void RegisterProducer(string producerId, ITcpConnection connection)
        {
            _producerDict.AddOrUpdate(producerId, key =>
            {
                _logger.InfoFormat("Producer registered, producerId: {0}", key);
                return new ProducerHeartbeatInfo(connection) { LastHeartbeatTime = DateTime.Now };
            }, (key, existing) =>
            {
                existing.LastHeartbeatTime = DateTime.Now;
                return existing;
            });
        }
        public void RemoveProducer(string producerId)
        {
            ProducerHeartbeatInfo heartbeatInfo;
            if (_producerDict.TryRemove(producerId, out heartbeatInfo))
            {
                try
                {
                    heartbeatInfo.Connection.Close();
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("Close tcp connection for producer client failed, producerId: {0}", producerId), ex);
                }
                _logger.InfoFormat("Producer removed, producerId: {0}, lastHeartbeat time: {1}", producerId, heartbeatInfo.LastHeartbeatTime);
            }
        }
        public int GetProducerCount()
        {
            return _producerDict.Count;
        }
        public IEnumerable<string> GetAllProducers()
        {
            return _producerDict.Keys.ToList();
        }
        public bool IsProducerExist(string producerId)
        {
            return _producerDict.ContainsKey(producerId);
        }

        private void ScanNotActiveProducer()
        {
            foreach (var entry in _producerDict)
            {
                if ((DateTime.Now - entry.Value.LastHeartbeatTime).TotalMilliseconds >= BrokerController.Instance.Setting.ProducerExpiredTimeout)
                {
                    RemoveProducer(entry.Key);
                }
            }
        }
    }
}
