using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EQueue.Common;
using EQueue.Common.Logging;

namespace EQueue.Clients.Producers
{
    public class Producer : IProducer
    {
        private readonly Client _client;
        private readonly ILogger _logger;

        public string GroupName { get; private set; }

        #region Constructors

        public Producer(
            Client client,
            string groupName,
            ILoggerFactory loggerFactory)
        {
            GroupName = groupName;

            _client = client;
            _logger = loggerFactory.Create(GetType().Name);
        }

        #endregion

        public void Start()
        {
            throw new NotImplementedException();
        }

        public void Shutdown()
        {
            throw new NotImplementedException();
        }

        public SendResult Send(Message message, object hashKey)
        {
            throw new NotImplementedException();
        }
        public Task<SendResult> SendAsync(Message message, object hashKey)
        {
            throw new NotImplementedException();
        }
    }
}
