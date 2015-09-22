namespace EQueue.Broker.Storage
{
    public struct RecordReadResult
    {
        public static readonly RecordReadResult Failure = new RecordReadResult(false, null);

        public readonly bool Success;
        public readonly ILogRecord LogRecord;

        public RecordReadResult(bool success, ILogRecord logRecord)
        {
            Success = success;
            LogRecord = logRecord;
        }

        public override string ToString()
        {
            return string.Format("[Success:{0}, LogRecord:{1}]", Success, LogRecord);
        }
    }
}
