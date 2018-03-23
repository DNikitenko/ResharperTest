using System;
using log4net.Core;

namespace Log4Net.Async
{
    public sealed class LoggingEventHelper
    {
        // needs to be a seperate class so that location is determined correctly by log4net when required

        private static readonly Type m_HelperType = typeof(LoggingEventHelper);
        private readonly string m_LoggerName;

        public FixFlags Fix { get; set; }

        public LoggingEventHelper(string loggerName, FixFlags fix)
        {
            m_LoggerName = loggerName;
            Fix = fix;
        }

        public LoggingEvent CreateLoggingEvent(Level level, string message, Exception exception)
        {
            var loggingEvent = new LoggingEvent(m_HelperType, null, m_LoggerName, level, message, exception)
            {
                Fix = Fix
            };
            return loggingEvent;
        }
    }
}