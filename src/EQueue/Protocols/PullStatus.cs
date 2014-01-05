namespace EQueue.Protocols
{
    public enum PullStatus
    {
        Found,
        NoNewMessage,
        OffsetIllegal,
        Failed
    }
}
