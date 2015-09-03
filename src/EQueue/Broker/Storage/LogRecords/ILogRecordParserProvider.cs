using System;
using System.Collections.Generic;
using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface ILogRecordParserProvider
    {
        ILogRecordParser GetLogRecordParser(byte recordType);
    }
}
