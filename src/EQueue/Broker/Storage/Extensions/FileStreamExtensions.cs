using System;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;
using ECommon.Components;
using ECommon.Logging;
using Microsoft.Win32.SafeHandles;

namespace EQueue.Broker.Storage
{
    public static class FileStreamExtensions
    {
        private static readonly ILogger _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(FileStreamExtensions));

        private static Action<FileStream> FlushSafe;
        private static Func<FileStream, SafeFileHandle> GetFileHandle;

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool FlushFileBuffers(SafeFileHandle hFile);

        static FileStreamExtensions()
        {
            ConfigureFlush(disableFlushToDisk: false);
        }

        public static void FlushToDisk(this FileStream fs)
        {
            FlushSafe(fs);
        }

        public static void ConfigureFlush(bool disableFlushToDisk)
        {
            try
            {
                ParameterExpression arg = Expression.Parameter(typeof(FileStream), "f");
                Expression expr = Expression.Field(arg, typeof(FileStream).GetField("_handle", BindingFlags.Instance | BindingFlags.NonPublic));
                GetFileHandle = Expression.Lambda<Func<FileStream, SafeFileHandle>>(expr, arg).Compile();
                FlushSafe = f =>
                {
                    f.Flush(flushToDisk: false);
                    if (!FlushFileBuffers(GetFileHandle(f)))
                    {
                        throw new Exception(string.Format("FlushFileBuffers failed with err: {0}", Marshal.GetLastWin32Error()));
                    }
                };
            }
            catch (Exception ex)
            {
                _logger.Error("Error while compiling sneaky SafeFileHandle getter.", ex);
                FlushSafe = f => f.Flush(flushToDisk: true);
            }
        }
    }
}
