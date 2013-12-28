using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EQueue.Broker
{
    public class BrokerController
    {
        private readonly IMessageService _messageService;

        public BrokerController(IMessageService messageService)
        {
            _messageService = messageService;
        }

        public void Initialize()
        {

        }
        public void Start()
        {

        }
        public void Shutdown()
        {

        }
    }
}
