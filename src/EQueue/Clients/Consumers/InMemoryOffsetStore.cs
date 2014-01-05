using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EQueue.Clients.Consumers
{
    public class InMemoryOffsetStore : IOffsetStore
    {
        public void Load()
        {

        }

        public void UpdateOffset(Protocols.MessageQueue messageQueue, long offset)
        {

        }

        public long ReadOffset(Protocols.MessageQueue messageQueue, OffsetReadType readType)
        {
            return 0L;
        }

        public void Persist(Protocols.MessageQueue messageQueue)
        {

        }

        public void RemoveOffset(Protocols.MessageQueue messageQueue)
        {

        }
    }
}
