using System.Threading.Tasks;
using EQueue.Common;

namespace EQueue.Clients.Producers
{
    public interface IProducer
    {
        SendResult Send(Message message, object arg);
        Task<SendResult> SendAsync(Message message, object arg);
    }
}
