namespace EQueue.Infrastructure
{
    public interface IJsonSerializer
    {
        string Serialize(object obj);
        T Deserialize<T>(string value) where T : class;
    }
}
