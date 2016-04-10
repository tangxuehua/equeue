using System;
using System.IO;
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
        /// <summary>Producer，Consumer对Broker发送的发消息和拉消息除外的其他内部请求，以及后台管理控制台发送的查询请求使用的端口号，默认为5002
        /// </summary>
        public IPEndPoint AdminAddress { get; set; }
        /// <summary>消息到达时是否立即通知相关的PullRequest，默认为true；
        /// <remarks>
        /// 如果希望当前场景消息吞吐量不大且要求消息消费的实时性更高，可以考虑设置为true；设置为false时，最多在<see cref="CheckBlockingPullRequestMilliseconds"/>
        /// 的时间后，PullRequest会被通知到有新消息；也就是说，设置为false时，消息最多延迟<see cref="CheckBlockingPullRequestMilliseconds"/>。
        /// </remarks>
        /// </summary>
        public bool NotifyWhenMessageArrived { get; set; }
        /// <summary>删除符合删除条件的消息的定时间隔，默认为10秒钟；
        /// </summary>
        public int DeleteMessagesInterval { get; set; }
        /// <summary>删除符合删除条件的队列中的消息索引的定时间隔；一定是消息先被删除后，该消息索引才会从队列中删除；默认为10秒钟；
        /// </summary>
        public int DeleteQueueMessagesInterval { get; set; }
        /// <summary>持久化消息消费进度的间隔，默认为1s；
        /// </summary>
        public int PersistConsumeOffsetInterval { get; set; }
        /// <summary>扫描PullRequest对应的队列是否有新消息的时间间隔，默认为1s；
        /// </summary>
        public int CheckBlockingPullRequestMilliseconds { get; set; }
        /// <summary>判断生产者者不在线的超时时间，默认为30s；即如果一个生产者30s不发送心跳到Broker，则认为不在线；Broker自动会关闭该生产者的连接并从生产者列表中移除；
        /// </summary>
        public int ProducerExpiredTimeout { get; set; }
        /// <summary>判断消费者心跳的超时时间，默认为30s；即如果一个消费者30s不发送心跳到Broker，则认为不在线；Broker自动会关闭该消费者的连接并从消费者列表中移除；
        /// </summary>
        public int ConsumerExpiredTimeout { get; set; }
        /// <summary>当消费者的链接断开时，是否需要立即将消费者从消费者列表中移除；如果为True，则消费者链接断开时立即移除；否则，会等到消费者心跳的超时时间到达后才会移除该消费者。默认为False
        /// </summary>
        public bool RemoveConsumerWhenDisconnect { get; set; }
        /// <summary>是否自动创建Topic，默认为true；线上环境建议设置为false，Topic应该总是由后台管理控制台来创建；
        /// </summary>
        public bool AutoCreateTopic { get; set; }
        /// <summary>创建Topic时，默认创建的队列数，默认为4；
        /// </summary>
        public int TopicDefaultQueueCount { get; set; }
        /// <summary>一个Topic下最多允许的队列数，默认为64；
        /// </summary>
        public int TopicMaxQueueCount { get; set; }
        /// <summary>消息最大允许的字节数，默认为4MB；
        /// </summary>
        public int MessageMaxSize { get; set; }
        /// <summary>EQueue存储文件的根目录
        /// </summary>
        public string FileStoreRootPath { get; private set; }
        /// <summary>TCP通行层设置
        /// </summary>
        public SocketSetting SocketSetting { get; set; }
        /// <summary>消息文件存储的相关配置，默认一个消息文件的大小为256MB
        /// </summary>
        public ChunkManagerConfig MessageChunkConfig { get; set; }
        /// <summary>队列文件存储的相关配置，默认一个队列文件中存储100W个消息索引，每个消息索引8个字节
        /// </summary>
        public ChunkManagerConfig QueueChunkConfig { get; set; }

        public BrokerSetting(string chunkFileStoreRootPath = @"c:\equeue-store", int messageChunkDataSize = 256 * 1024 * 1024, int chunkFlushInterval = 100, int chunkCacheMaxPercent = 75, int chunkCacheMinPercent = 40, int maxLogRecordSize = 5 * 1024 * 1024, int chunkWriteBuffer = 128 * 1024, int chunkReadBuffer = 128 * 1024, bool syncFlush = false, bool enableCache = true)
        {
            ProducerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5000);
            ConsumerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5001);
            AdminAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5002);

            NotifyWhenMessageArrived = true;
            DeleteMessagesInterval = 1000 * 10;
            DeleteQueueMessagesInterval = 1000 * 10;
            PersistConsumeOffsetInterval = 1000 * 1;
            CheckBlockingPullRequestMilliseconds = 1000 * 1;
            ProducerExpiredTimeout = 1000 * 30;
            ConsumerExpiredTimeout = 1000 * 30;
            RemoveConsumerWhenDisconnect = false;
            AutoCreateTopic = true;
            TopicDefaultQueueCount = 4;
            TopicMaxQueueCount = 64;
            MessageMaxSize = 1024 * 1024 * 4;
            FileStoreRootPath = chunkFileStoreRootPath;
            MessageChunkConfig = new ChunkManagerConfig(
                Path.Combine(chunkFileStoreRootPath, @"message-chunks"),
                new DefaultFileNamingStrategy("message-chunk-"),
                messageChunkDataSize,
                0,
                0,
                chunkFlushInterval,
                enableCache,
                syncFlush,
                Environment.ProcessorCount * 2,
                maxLogRecordSize,
                chunkWriteBuffer,
                chunkReadBuffer,
                chunkCacheMaxPercent,
                chunkCacheMinPercent,
                1,
                5,
                300000,
                true,
                true);
            QueueChunkConfig = new ChunkManagerConfig(
                Path.Combine(chunkFileStoreRootPath, @"queue-chunks"),
                new DefaultFileNamingStrategy("queue-chunk-"),
                0,
                12,
                1000000,
                chunkFlushInterval,
                enableCache,
                syncFlush,
                Environment.ProcessorCount * 2,
                12,
                chunkWriteBuffer,
                chunkReadBuffer,
                chunkCacheMaxPercent,
                chunkCacheMinPercent,
                1,
                5,
                1000000,
                false,
                false);
        }
    }
}
