using System.Threading.Tasks;
using EQueue.Common;

namespace EQueue.Clients.Producers
{
    public interface IProducer
    {
        SendResult Send(Message message, string arg);
        Task<SendResult> SendAsync(Message message, string arg);
    }
}
