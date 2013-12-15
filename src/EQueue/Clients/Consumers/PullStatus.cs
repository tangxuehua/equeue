namespace EQueue.Clients.Consumers
{
    public enum PullStatus
    {
        FOUND,
        NO_NEW_MSG,
        OFFSET_ILLEGAL
    }
}
