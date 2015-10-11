using System;

namespace EQueue.Broker.Storage
{
    public interface IFileNamingStrategy
    {
        string GetFileNameFor(string path, int index);
        string[] GetChunkFiles(string path);
        string[] GetTempFiles(string path);
    }
}
