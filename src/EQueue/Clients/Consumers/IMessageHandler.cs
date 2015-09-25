using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public interface IMessageHandler
    {
        void Handle(MessageLogRecord message, IMessageContext context);
    }
}
