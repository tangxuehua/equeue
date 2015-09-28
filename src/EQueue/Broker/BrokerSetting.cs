using System.Net;
using ECommon.Socketing;
using EQueue.Broker.Storage;

namespace EQueue.Broker
{
    public class BrokerSetting
    {
        /// <summary>发送消息端口号，默认为5000
        /// </summary>
        public IPEndPoint ProducerAddress { get; set; }
        /// <summary>消费消息端口号，默认为5001
        /// </summary>
        public IPEndPoint ConsumerAddress { get; set; }
        /// <summary>后台管理控制台使用的端口号，默认为5002
        /// </summary>
        public IPEndPoint AdminAddress { get; set; }
        /// <summary>消息到达时是否立即通知相关的PullRequest，默认为false；
        /// <remarks>
        /// 如果希望当前场景消息吞吐量不大且要求消息消费的实时性更高，可以考虑设置为true；设置为false时，最多在<see cref="CheckBlockingPullRequestMilliseconds"/>
        /// 的时间后，PullRequest会被通知到有新消息；也就是说，设置为false时，最多延迟<see cref="CheckBlockingPullRequestMilliseconds"/>会被通知到有新消息。
        /// </remarks>
        /// </summary>
        public bool NotifyWhenMessageArrived { get; set; }
        /// <summary>删除符合删除条件的消息的定时间隔，默认为10分钟；
        /// </summary>
        public int DeleteMessagesInterval { get; set; }
        /// <summary>删除符合删除条件的队列中的消息索引的定时间隔；一定是消息先被删除后，该消息索引才会从队列中删除；默认为10分钟；
        /// </summary>
        public int DeleteQueueMessagesInterval { get; set; }
        /// <summary>持久化消息消费进度的间隔，默认为1s；
        /// </summary>
        public int PersistConsumeOffsetInterval { get; set; }
        /// <summary>扫描PullRequest对应的队列是否有新消息的时间间隔，默认为1s；
        /// </summary>
        public int CheckBlockingPullRequestMilliseconds { get; set; }
        /// <summary>判断消费者不在线的超时时间，默认为60s；即如果一个消费者60s不发送心跳到Broker，则认为不在线；Broker自动会关闭该消费者的连接并从消费者列表中移除；
        /// </summary>
        public int ConsumerExpiredTimeout { get; set; }
        /// <summary>是否自动创建Topic，默认为true；线上环境建议设置为false，Topic应该总是由后台管理控制台来创建；
        /// </summary>
        public bool AutoCreateTopic { get; set; }
        /// <summary>创建Topic时，默认创建的队列数，默认为4；
        /// </summary>
        public int TopicDefaultQueueCount { get; set; }
        /// <summary>一个Topic下最多允许的队列数，默认为64；
        /// </summary>
        public int TopicMaxQueueCount { get; set; }
        /// <summary>消息文件存储的相关配置，默认一个消息文件的大小为256MB
        /// </summary>
        public TFChunkManagerConfig MessageChunkConfig { get; set; }
        /// <summary>队列文件存储的相关配置，默认一个队列文件中存储100W个消息索引，每个消息索引8个字节
        /// </summary>
        public TFChunkManagerConfig QueueChunkConfig { get; set; }
        /// <summary>文件存储路径根目录
        /// </summary>
        public string RootStorePath { get; set; }

        public BrokerSetting()
        {
            ProducerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5000);
            ConsumerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5001);
            AdminAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5002);

            NotifyWhenMessageArrived = false;
            DeleteMessagesInterval = 1000 * 60 * 10;
            DeleteQueueMessagesInterval = 1000 * 60 * 10;
            PersistConsumeOffsetInterval = 1000 * 1;
            CheckBlockingPullRequestMilliseconds = 1000 * 1;
            ConsumerExpiredTimeout = 1000 * 60;
            AutoCreateTopic = true;
            TopicDefaultQueueCount = 4;
            TopicMaxQueueCount = 64;

            RootStorePath = @"c:\equeue-store";
            MessageChunkConfig = TFChunkManagerConfig.Create(RootStorePath + @"\message-chunks", "message-chunk-", 256 * 1024 * 1024, 0, 0);
            QueueChunkConfig = TFChunkManagerConfig.Create(RootStorePath + @"\queue-chunks", "queue-chunk-", 0, 8, 1000000);
        }
    }
}
