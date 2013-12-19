namespace EQueue.Clients.Consumers
{
    public enum PullStatus
    {
        Found,
        NoNewMessage,
        OffsetIllegal
    }
}
