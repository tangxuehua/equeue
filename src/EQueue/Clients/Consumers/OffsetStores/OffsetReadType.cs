namespace EQueue.Clients.Consumers.OffsetStores
{
    public enum OffsetReadType
    {
        ReadFromMemory,
        ReadFromStore,
        MemoryFirstThenStore
    }
}
