using ECommon.Remoting;
using ECommon.Socketing;

namespace EQueue.Broker
{
    public class BrokerSetting
    {
        public SocketSetting ProducerSocketSetting { get; set; }
        public SocketSetting ConsumerSocketSetting { get; set; }
        public bool NotifyWhenMessageArrived { get; set; }
        public int DeleteMessageInterval { get; set; }
        public int SuspendPullRequestMilliseconds { get; set; }
        public int DefaultTopicQueueCount { get; set; }

        public BrokerSetting()
        {
            ProducerSocketSetting = new SocketSetting { Address = SocketUtils.GetLocalIPV4().ToString(), Port = 5000, Backlog = 5000 };
            ConsumerSocketSetting = new SocketSetting { Address = SocketUtils.GetLocalIPV4().ToString(), Port = 5001, Backlog = 5000 };
            NotifyWhenMessageArrived = true;
            DeleteMessageInterval = 1000 * 60 * 60;
            SuspendPullRequestMilliseconds = 1000 * 60;
            DefaultTopicQueueCount = 4;
        }
    }
}
