namespace EQueue.Protocols
{
    public enum PullStatus
    {
        Found = 1,
        NoNewMessage = 2,
        NextOffsetReset = 3,
        Ignored = 4
    }
}
