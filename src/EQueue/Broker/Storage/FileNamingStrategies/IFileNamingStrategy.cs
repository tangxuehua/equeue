using System;

namespace EQueue.Broker.Storage
{
    public interface IFileNamingStrategy
    {
        string GetFilenameFor(int index, int version);
        string DetermineBestVersionFilenameFor(int index);
        string[] GetAllVersionsFor(int index);
        string[] GetAllPresentFiles();

        string GetTempFilename();
        string[] GetAllTempFiles();
    }
}
