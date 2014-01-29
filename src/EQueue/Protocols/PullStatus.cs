namespace EQueue.Protocols
{
    public enum PullStatus
    {
        Found = 1,
        NoNewMessage = 2,
        OffsetIllegal = 3,
        Ignored = 4,
        Failed = 100
    }
}
