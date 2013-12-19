using System.Threading.Tasks;
using EQueue.Common;

namespace EQueue.Clients.Producers
{
    public interface IProducer
    {
        SendResult Send(Message message, object hashKey);
        Task<SendResult> SendAsync(Message message, object hashKey);
    }
}
