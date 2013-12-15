namespace EQueue.Clients.Consumers
{
    public enum OffsetReadType
    {
        ReadFromMemory,
        ReadFromStore,
        MemoryFirstThenStore
    }
}
