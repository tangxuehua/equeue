using System;
using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class MessageContext : IMessageContext
    {
        public Action<MessageLogRecord> MessageHandledAction { get; private set; }

        public MessageContext(Action<MessageLogRecord> messageHandledAction)
        {
            MessageHandledAction = messageHandledAction;
        }

        public void OnMessageHandled(MessageLogRecord queueMessage)
        {
            MessageHandledAction(queueMessage);
        }
    }
}
