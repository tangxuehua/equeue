using System;

namespace EQueue.Broker.Storage
{
    public interface IFileNamingStrategy
    {
        string GetFileNameFor(string path, int index);
        string[] GetAllFiles(string path);
    }
}
