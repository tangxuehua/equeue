using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class InMemoryOffsetStore : IOffsetStore
    {
        public void Load()
        {

        }

        public void UpdateOffset(MessageQueue messageQueue, long offset)
        {

        }

        public long ReadOffset(MessageQueue messageQueue, OffsetReadType readType)
        {
            return 0L;
        }

        public void Persist(MessageQueue messageQueue)
        {

        }

        public void RemoveOffset(MessageQueue messageQueue)
        {

        }
    }
}
