using System;
using System.Collections.Generic;

namespace EQueue.Broker.Exceptions
{
    public class BrokerCleanningException : Exception
    {
        public BrokerCleanningException() : base("Broker is currently cleanning.") { }
    }
}
