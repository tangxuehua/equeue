using System;
using System.Collections.Generic;
using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class DefaultLogRecordParserProvider : ILogRecordParserProvider
    {
        private readonly IDictionary<byte, ILogRecordParser> _parserDict = new Dictionary<byte, ILogRecordParser>();

        public DefaultLogRecordParserProvider RegisterParser(byte recordType, ILogRecordParser parser)
        {
            _parserDict.Add(recordType, parser);
            return this;
        }
        public ILogRecordParser GetLogRecordParser(byte recordType)
        {
            return _parserDict[recordType];
        }
    }
}
