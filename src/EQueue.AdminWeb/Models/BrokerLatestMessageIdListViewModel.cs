using System;
using System.Collections.Generic;

namespace EQueue.AdminWeb.Models
{
    public class BrokerLatestMessageIdListViewModel
    {
        public IEnumerable<MessageInfo> MessageInfoList { get; set; }
    }
    public class MessageInfo
    {
        public int Sequence;
        public string MessageId;
        public DateTime CreateTime;
        public DateTime StoredTime;
    }
}