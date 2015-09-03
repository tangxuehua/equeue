using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EQueue.Broker.Storage.Exceptions
{
    public class ExtraneousFileFoundException : Exception
    {
        public ExtraneousFileFoundException(string message) : base(message)
        {

        }
    }
}
