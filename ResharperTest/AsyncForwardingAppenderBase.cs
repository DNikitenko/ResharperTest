namespace Log4Net.Async
{
    using log4net.Appender;
    using log4net.Core;
    using log4net.Util;
    using System;
    using System.Runtime.Remoting.Messaging;

    public abstract class AsyncForwardingAppenderBase : ForwardingAppender
    {
        #region Private Members

        private const FixFlags DEFAULT_FIX_FLAGS = FixFlags.Partial;
        private FixFlags m_FixFlags = DEFAULT_FIX_FLAGS;
        private LoggingEventHelper m_LoggingEventHelper;

        #endregion Private Members

        #region Properties

        public FixFlags Fix
        {
            get { return m_FixFlags; }
            set { SetFixFlags(value); }
        }

        /// <summary>
        /// Returns HttpContext.Current
        /// </summary>
        protected internal object HttpContext
        {
            get
            {
                return CallContext.HostContext;
            }
            set
            {
                CallContext.HostContext = value;
            }
        }

        /// <summary>
        /// The logger name that will be used for logging internal errors.
        /// </summary>
        protected abstract string InternalLoggerName { get; }

        public abstract int BufferSize { get; set; }

        #endregion Properties

        public override void ActivateOptions()
        {
            base.ActivateOptions();
            m_LoggingEventHelper = new LoggingEventHelper(InternalLoggerName, Fix);
            InitializeAppenders();
        }

        #region Appender Management

        public override void AddAppender(IAppender newAppender)
        {
            base.AddAppender(newAppender);
            SetAppenderFixFlags(newAppender);
        }

        private void SetFixFlags(FixFlags newFixFlags)
        {
            if (newFixFlags != m_FixFlags)
            {
                if (m_LoggingEventHelper != null)
                {
                    m_LoggingEventHelper.Fix = newFixFlags;
                }
                m_FixFlags = newFixFlags;
                InitializeAppenders();
            }
        }

        private void InitializeAppenders()
        {
            foreach (var appender in Appenders)
            {
                SetAppenderFixFlags(appender);
            }
        }

        private void SetAppenderFixFlags(IAppender appender)
        {
            var bufferingAppender = appender as BufferingAppenderSkeleton;
            if (bufferingAppender != null)
            {
                bufferingAppender.Fix = Fix;
            }
        }

        #endregion Appender Management

        #region Forwarding

        protected void ForwardInternalError(string message, Exception exception, Type thisType)
        {
            LogLog.Error(thisType, message, exception);
            var loggingEvent = m_LoggingEventHelper.CreateLoggingEvent(Level.Error, message, exception);
            ForwardLoggingEvent(loggingEvent, thisType);
        }

        protected void ForwardLoggingEvent(LoggingEvent loggingEvent, Type thisType)
        {
            try
            {
                base.Append(loggingEvent);
            }
            catch (Exception exception)
            {
                LogLog.Error(thisType, "Unable to forward logging event", exception);
            }
        }

        #endregion Forwarding
    }
}
