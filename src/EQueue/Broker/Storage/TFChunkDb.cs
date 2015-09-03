using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Utilities;
using EQueue.Broker.Storage.Exceptions;

namespace EQueue.Broker.Storage
{
    public class TFChunkDb : IDisposable
    {
        private static readonly ILogger _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(TFChunkDb));

        public readonly TFChunkDbConfig Config;
        public readonly TFChunkManager Manager;

        public TFChunkDb(TFChunkDbConfig config)
        {
            Ensure.NotNull(config, "config");

            Config = config;
            Manager = new TFChunkManager(Config);
        }

        public void Open(bool readOnly = false)
        {
            var checkpoint = Config.WriterCheckpoint.Read();

            if (Config.InMemDb)
            {
                Manager.AddNewChunk();
                return;
            }

            var lastChunkNum = (int)(checkpoint / Config.ChunkDataSize);
            var lastChunkVersions = Config.FileNamingStrategy.GetAllVersionsFor(lastChunkNum);

            for (int chunkNum = 0; chunkNum < lastChunkNum; )
            {
                var versions = Config.FileNamingStrategy.GetAllVersionsFor(chunkNum);
                if (versions.Length == 0)
                    throw new CorruptDatabaseException(new ChunkNotFoundException(Config.FileNamingStrategy.GetFilenameFor(chunkNum, 0)));

                TFChunk chunk;
                if (lastChunkVersions.Length == 0 && (chunkNum + 1) * (long)Config.ChunkDataSize == checkpoint)
                {
                    // The situation where the logical data size is exactly divisible by ChunkSize,
                    // so it might happen that we have checkpoint indicating one more chunk should exist, 
                    // but the actual last chunk is (lastChunkNum-1) one and it could be not completed yet -- perfectly valid situation.
                    var footer = ReadChunkFooter(versions[0]);
                    if (footer.IsCompleted)
                        chunk = TFChunk.FromCompletedFile(versions[0]);
                    else
                    {
                        chunk = TFChunk.FromOngoingFile(versions[0], Config.ChunkDataSize, checkSize: false);
                        // chunk is full with data, we should complete it right here
                        if (!readOnly)
                            chunk.Complete();
                    }
                }
                else
                {
                    chunk = TFChunk.FromCompletedFile(versions[0]);
                }
                Manager.AddChunk(chunk);
                chunkNum = chunk.ChunkHeader.ChunkEndNumber + 1;
            }

            if (lastChunkVersions.Length == 0)
            {
                var onBoundary = checkpoint == (Config.ChunkDataSize * (long)lastChunkNum);
                if (!onBoundary)
                    throw new CorruptDatabaseException(new ChunkNotFoundException(Config.FileNamingStrategy.GetFilenameFor(lastChunkNum, 0)));
                if (!readOnly)
                    Manager.AddNewChunk();
            }
            else
            {
                var chunkFileName = lastChunkVersions[0];
                var chunkHeader = ReadChunkHeader(chunkFileName);
                var chunkLocalDataPosition = chunkHeader.GetLocalDataPosition(checkpoint);
                var lastChunk = TFChunk.FromOngoingFile(chunkFileName, chunkLocalDataPosition, checkSize: false);
                Manager.AddChunk(lastChunk);
            }

            EnsureNoExcessiveChunks(lastChunkNum);

            if (!readOnly)
            {
                RemoveOldChunksVersions(lastChunkNum);
                CleanUpTempFiles();
            }
        }

        private static ChunkHeader ReadChunkHeader(string chunkFileName)
        {
            ChunkHeader chunkHeader;
            using (var fs = new FileStream(chunkFileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                if (fs.Length < ChunkFooter.Size + ChunkHeader.Size)
                {
                    throw new CorruptDatabaseException(new BadChunkInDatabaseException(
                        string.Format("Chunk file '{0}' is bad. It even doesn't have enough size for header and footer, file size is {1} bytes.",
                                      chunkFileName, fs.Length)));
                }
                chunkHeader = ChunkHeader.FromStream(fs);
            }
            return chunkHeader;
        }
        private static ChunkFooter ReadChunkFooter(string chunkFileName)
        {
            ChunkFooter chunkFooter;
            using (var fs = new FileStream(chunkFileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                if (fs.Length < ChunkFooter.Size + ChunkHeader.Size)
                {
                    throw new CorruptDatabaseException(new BadChunkInDatabaseException(
                        string.Format("Chunk file '{0}' is bad. It even doesn't have enough size for header and footer, file size is {1} bytes.",
                                      chunkFileName, fs.Length)));
                }
                fs.Seek(-ChunkFooter.Size, SeekOrigin.End);
                chunkFooter = ChunkFooter.FromStream(fs);
            }
            return chunkFooter;
        }
        private void EnsureNoExcessiveChunks(int lastChunkNum)
        {
            var allowedFiles = new List<string>();
            int cnt = 0;
            for (int i = 0; i <= lastChunkNum; ++i)
            {
                var files = Config.FileNamingStrategy.GetAllVersionsFor(i);
                cnt += files.Length;
                allowedFiles.AddRange(files);
            }

            var allFiles = Config.FileNamingStrategy.GetAllPresentFiles();
            if (allFiles.Length != cnt)
            {
                throw new CorruptDatabaseException(new ExtraneousFileFoundException(
                    string.Format("Unexpected files: {0}.", string.Join(", ", allFiles.Except(allowedFiles)))));
            }
        }
        private void RemoveOldChunksVersions(int lastChunkNum)
        {
            for (int chunkNum = 0; chunkNum <= lastChunkNum; )
            {
                var chunk = Manager.GetChunk(chunkNum);
                for (int i = chunk.ChunkHeader.ChunkStartNumber; i <= chunk.ChunkHeader.ChunkEndNumber; ++i)
                {
                    var files = Config.FileNamingStrategy.GetAllVersionsFor(i);
                    for (int j = (i == chunk.ChunkHeader.ChunkStartNumber ? 1 : 0); j < files.Length; ++j)
                    {
                        RemoveFile("Removing excess chunk version: {0}...", files[j]);
                    }
                }
                chunkNum = chunk.ChunkHeader.ChunkEndNumber + 1;
            }
        }
        private void CleanUpTempFiles()
        {
            var tempFiles = Config.FileNamingStrategy.GetAllTempFiles();
            foreach (string tempFile in tempFiles)
            {
                try
                {
                    RemoveFile("Deleting temporary file {0}...", tempFile);
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("Error while trying to delete remaining temp file: '{0}'.", tempFile), ex);
                }
            }
        }
        private void RemoveFile(string reason, string file)
        {
            _logger.InfoFormat(reason, file);
            File.SetAttributes(file, FileAttributes.Normal);
            File.Delete(file);
        }

        public void Dispose()
        {
            Close();
        }
        public void Close()
        {
            if (Manager != null)
                Manager.Dispose();
            Config.WriterCheckpoint.Close();
        }
    }
}
