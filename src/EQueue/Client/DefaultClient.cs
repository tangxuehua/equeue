using System;
using System.Collections.Concurrent;
using System.Threading;
using EQueue.Client.Consumer;
using EQueue.Client.Producer;
using EQueue.Common;
using EQueue.Common.Logging;

namespace EQueue.Client
{
    public class DefaultClient
    {
        private readonly ConcurrentDictionary<string, IProducer> _producerDict = new ConcurrentDictionary<string, IProducer>();
        private readonly ConcurrentDictionary<string, IConsumer> _consumerDict = new ConcurrentDictionary<string, IConsumer>();
        private readonly ConcurrentDictionary<string, TopicRouteData> _topicRouteDataDict = new ConcurrentDictionary<string, TopicRouteData>();
        private readonly IPullMessageService _pullMessageService;
        private readonly ILogger _logger;
        private readonly string _clientId;
        private readonly ClientConfig _config;
        private Timer _timer;

        public DefaultClient(string clientId, ClientConfig config, IPullMessageService pullMessageService, ILoggerFactory loggerFactory)
        {
            _clientId = clientId;
            _config = config;
            _pullMessageService = pullMessageService;
            _logger = loggerFactory.Create(GetType().Name);
            _logger.InfoFormat("A new mq client create, ClinetID: {0}, Config:{1}", _clientId, _config);
        }

        public void Start()
        {
            _pullMessageService.Start();
            StartRebalance();
        }

        public IConsumer SelectConsumer(PullRequest pullRequest)
        {
            //TODO
            return null;
        }



        private void StartRebalance()
        {
            if (_timer == null)
            {
                _timer = new Timer((obj) =>
                {
                    foreach (var consumer in _consumerDict.Values)
                    {
                        try
                        {
                            consumer.DoRebalance();
                        }
                        catch (Exception ex)
                        {
                            _logger.Error("Rebalance has exception.", ex);
                        }
                    }
                }, null, 1000 * 10, 1000 * 10);
            }
        }
    }
}
