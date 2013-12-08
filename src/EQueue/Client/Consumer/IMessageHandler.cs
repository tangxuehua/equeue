using EQueue.Common;

namespace EQueue.Client.Consumer
{
    public interface IMessageHandler
    {
        void Handle(Message message);
    }
}
