using System;
using System.Text;
using EQueue.Clients.Consumers;
using EQueue.Protocols;

namespace QuickStart.ConsumerClient
{
    class Program
    {
        static void Main(string[] args)
        {
            var consumer = new Consumer(ConsumerSettings.Default, "group1", MessageModel.Clustering, new MessageHandler())
                .Subscribe("topic1")
                .Subscribe("topic2")
                .Start();
            Console.ReadLine();
        }
    }

    class MessageHandler : IMessageHandler
    {
        public void Handle(Message message)
        {
            var content = Encoding.UTF8.GetString(message.Body);
            Console.WriteLine("Topic:{0}, Message:{1}", message.Topic, content);
        }
    }
}
