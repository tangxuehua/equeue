using System.Collections.Generic;
using System.Threading.Tasks;
using EQueue.Common;

namespace EQueue.Client.Producer
{
    public interface IProducer
    {
        IEnumerable<string> PublishTopics { get; }
        void Start();
        void Shutdown();
        Task<SendResult> Send(Message message);
        bool IsPublishTopicNeedUpdate(string topic);
        void UpdateTopicPublishInfo(string topic, IEnumerable<MessageQueue> messageQueues);
    }
}
