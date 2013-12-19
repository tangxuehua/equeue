using System;
using System.Threading.Tasks;
using EQueue.Common;
using EQueue.Common.Logging;

namespace EQueue.Clients.Producers
{
    public class Producer : IProducer
    {
        private readonly string BrokerAddress;
        private readonly ILogger _logger;

        #region Constructors

        public Producer(string brokerAddress, ILoggerFactory loggerFactory)
        {
            BrokerAddress = brokerAddress;
            _logger = loggerFactory.Create(GetType().Name);
        }

        #endregion

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
