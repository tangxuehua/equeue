using System.Threading.Tasks;
using EQueue.Broker;
using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public interface IProducer
    {
        SendResult Send(Message message, string arg);
        Task<SendResult> SendAsync(Message message, string arg);
        SendResult Send(Message message, string arg, IQueueSelector queueSelector);
        Task<SendResult> SendAsync(Message message, string arg, IQueueSelector queueSelector);
    }
}
