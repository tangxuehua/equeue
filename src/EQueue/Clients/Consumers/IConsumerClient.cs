namespace EQueue.Clients.Consumers
{
    public interface IConsumerClient
    {
        IConsumer GetConsumer(string consumerGroup);
    }
}
