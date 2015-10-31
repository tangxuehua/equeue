using System;
using System.Net;
using ECommon.Socketing;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class ConsumerSetting
    {
        /// <summary>Broker的发送消息的地址
        /// </summary>
        public IPEndPoint BrokerAddress { get; set; }
        /// <summary>Broker处理管理请求的地址
        /// </summary>
        public IPEndPoint BrokerAdminAddress { get; set; }
        /// <summary>本地所绑定的地址，可以为空，开发者可以指定本地所使用的端口；
        /// </summary>
        public IPEndPoint LocalAddress { get; set; }
        /// <summary>本地管理所绑定的地址，可以为空，开发者可以指定本地所使用的端口；
        /// </summary>
        public IPEndPoint LocalAdminAddress { get; set; }
        /// <summary>Socket通信层相关的设置；
        /// </summary>
        public SocketSetting SocketSetting { get; set; }
        /// <summary>消费消息所使用的线程池大小，默认为当前服务器的CPU核数乘以2
        /// </summary>
        public int ConsumeThreadMaxCount { get; set; }
        /// <summary>消费者负载均衡的间隔，默认为1s；
        /// </summary>
        public int RebalanceInterval { get; set; }
        /// <summary>从Broker获取最新队列信息的间隔，默认为1s；
        /// </summary>
        public int UpdateTopicQueueCountInterval { get; set; }
        /// <summary>向Broker的心跳的间隔，默认为1s；
        /// </summary>
        public int HeartbeatBrokerInterval { get; set; }
        /// <summary>向Broker发送消费进度的间隔，默认为1s；
        /// </summary>
        public int SendConsumerOffsetInterval { get; set; }
        /// <summary>从Broker拉取消息时，开始流控的阀值，默认为1000；即当前拉取到本地未消费的消息数到达1000时，将开始做流控，减慢拉取速度；
        /// </summary>
        public int PullMessageFlowControlThreshold { get; set; }
        /// <summary>当拉取消息开始流控时，需要逐渐增加流控时间的步长百分比，默认为1%；
        /// <remarks>
        /// 假设当前本地拉取且并未消费的消息数超过阀值时，需要逐渐增加流控时间；具体增加多少时间取决于
        /// PullMessageFlowControlStepPercent以及PullMessageFlowControlStepWaitMilliseconds属性的配置值；
        /// 举个例子，假设流控阀值为1000，步长百分比为1%，每个步长等待时间为1ms；
        /// 然后，假如当前拉取到本地未消费的消息数为1200，
        /// 则超出阀值的消息数是：1200 - 1000 = 200，
        /// 步长为：1000 * 1% = 10；
        /// 然后，200 / 10 = 20，即当前超出的消息数是步长的20倍；
        /// 所以，最后需要等待的时间为20 * 1ms = 20ms;
        /// </remarks>
        /// </summary>
        public int PullMessageFlowControlStepPercent { get; set; }
        /// <summary>当拉取消息开始流控时，每个步长需要等待的时间，默认为1ms；
        /// </summary>
        public int PullMessageFlowControlStepWaitMilliseconds { get; set; }

        /// <summary>拉取消息TCP长轮训的周期，默认为60s；
        /// </summary>
        public int SuspendPullRequestMilliseconds { get; set; }
        /// <summary>拉取消息的请求的超时时间，必须大于长轮训的周期，默认为70s；
        /// </summary>
        public int PullRequestTimeoutMilliseconds { get; set; }
        /// <summary>重试处理出现异常（失败）的消息的时间间隔，一次重试一个处理失败的消息，默认为100毫秒；
        /// </summary>
        public int RetryMessageInterval { get; set; }
        /// <summary>一次从Broker拉取的消息的最大数量，默认为32个；
        /// </summary>
        public int PullMessageBatchSize { get; set; }
        /// <summary>消费者启动时，针对当前要消费的队列，如果Broker上之前没有保存过任何该队列的消费进度（一般是该消费者第一次启动），则通过该选项指定要从队列的什么位置开始消费；可以从队列的第一个消息开始消费，也可以从最后一个消息之后的后续的新消息开始消费；
        /// </summary>
        public ConsumeFromWhere ConsumeFromWhere { get; set; }

        public ConsumerSetting()
        {
            BrokerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5001);
            BrokerAdminAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5002);
            SocketSetting = new SocketSetting();
            ConsumeThreadMaxCount = Environment.ProcessorCount * 2;
            RebalanceInterval = 1000;
            HeartbeatBrokerInterval = 1000;
            UpdateTopicQueueCountInterval = 1000;
            SendConsumerOffsetInterval = 1000;
            PullMessageFlowControlThreshold = 1000;
            PullMessageFlowControlStepPercent = 1;
            PullMessageFlowControlStepWaitMilliseconds = 1;
            SuspendPullRequestMilliseconds = 60 * 1000;
            PullRequestTimeoutMilliseconds = 70 * 1000;
            RetryMessageInterval = 100;
            PullMessageBatchSize = 32;
            ConsumeFromWhere = ConsumeFromWhere.LastOffset;
        }
    }
}
