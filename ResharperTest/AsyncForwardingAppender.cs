using log4net.Core;
using System;
using System.Threading;

namespace Log4Net.Async
{
    public sealed class AsyncForwardingAppender : AsyncForwardingAppenderBase
    {
        private static readonly TimeSpan m_ShutdownFlushTimeout = TimeSpan.FromSeconds(5);
        private static readonly Type m_ThisType = typeof(AsyncForwardingAppender);
        private const int DEFAULT_BUFFER_SIZE = 1000;
        private DateTime m_EventDroppedNotificationSuppressionEnd = DateTime.MinValue;
        private Thread m_ForwardingThread;

        private readonly object m_BufferLock = new object();
        private volatile bool m_ShutDownRequested;

        private ConcurrentFifoQueue<LoggingEventContext> m_Buffer;

        private readonly ManualResetEvent m_ShuttingDown = new ManualResetEvent(false);
        private readonly AutoResetEvent m_EventArrived = new AutoResetEvent(false);

        private int m_NumDroppedLogEvents;

        private int m_BufferSize = DEFAULT_BUFFER_SIZE;

        private const int EVENT_DROPPED_NOTIFICATION_SUPPRESSION_TIME = 30;

        public override int BufferSize
        {
            get { return m_BufferSize; }
            set { SetBufferSize(value); }
        }

        protected override string InternalLoggerName
        {
            get
            {
                return "AsyncForwardingAppender";
            }
        }

        #region Startup

        public override void ActivateOptions()
        {
            base.ActivateOptions();
            InitializeBuffer();
            StartForwarding();
        }

        private void StartForwarding()
        {
            if (m_ShutDownRequested)
            {
                return;
            }

            m_ForwardingThread = new Thread(ForwardingThreadExecute)
            {
                Name = string.Format("{0} Forwarding Appender Thread", Name),
                IsBackground = false,
            };
            m_ForwardingThread.Start();
        }

        #endregion Startup

        #region Shutdown

        protected override void OnClose()
        {
            StopForwarding();
            base.OnClose();
        }

        private void StopForwarding()
        {
            m_ShutDownRequested = true;
            m_ShuttingDown.Set();
            var hasFinishedFlushingBuffer = m_ForwardingThread?.Join(m_ShutdownFlushTimeout) ?? true;

            if (!hasFinishedFlushingBuffer)
            {
                m_ForwardingThread.Abort();
                ForwardInternalError("Unable to flush the AsyncForwardingAppender buffer in the allotted time, forcing a shutdown", null, m_ThisType);
            }
        }

        #endregion Shutdown

        #region Appending

        protected override void Append(LoggingEvent loggingEvent)
        {
            if (!m_ShutDownRequested && loggingEvent != null)
            {
                loggingEvent.Fix = Fix;
                if (!m_Buffer.TryEnqueue(new LoggingEventContext(loggingEvent, HttpContext)))
                {
                    Interlocked.Increment(ref m_NumDroppedLogEvents);
                }
                m_EventArrived.Set();
            }
        }

        protected override void Append(LoggingEvent[] loggingEvents)
        {
            if (!m_ShutDownRequested && loggingEvents != null)
            {
                foreach (var loggingEvent in loggingEvents)
                {
                    Append(loggingEvent);
                }
            }
        }

        #endregion Appending

        #region Forwarding

        private void ForwardingThreadExecute()
        {
            while (!m_ShutDownRequested)
            {
                try
                {
                    ForwardLoggingEventsFromBuffer();
                }
                catch (Exception exception)
                {
                    ForwardInternalError("Unexpected error in asynchronous forwarding loop", exception, m_ThisType);
                }
            }
        }

        private void ForwardLoggingEventsFromBuffer()
        {
            var waitHandles = new WaitHandle[] { m_ShuttingDown, m_EventArrived };

            for (;;)
            {
                var which = WaitHandle.WaitAny(waitHandles);

                if (which < 0 || which >= waitHandles.Length)
                {
                    ForwardInternalError(string.Format(
                        "Unexpected index '{0}' of waitHandle has been returned", which),
                        null, m_ThisType);
                }
                else if (waitHandles[which] == m_EventArrived)
                {
                    HandleEventArrived();
                }
                else if (waitHandles[which] == m_ShuttingDown)
                {
                    break;
                }
            }
        }

        private void HandleEventArrived()
        {
            LoggingEventContext loggingEventContext;
            while (m_Buffer.TryDequeue(out loggingEventContext))
            {
                HttpContext = loggingEventContext.HttpContext;
                ForwardLoggingEvent(loggingEventContext.LoggingEvent, m_ThisType);


                if (m_NumDroppedLogEvents != 0 && m_EventDroppedNotificationSuppressionEnd <= DateTime.UtcNow)
                {
                    m_EventDroppedNotificationSuppressionEnd = DateTime.UtcNow.AddSeconds(EVENT_DROPPED_NOTIFICATION_SUPPRESSION_TIME);
                    HandleEventDropped();
                }
            }
        }

        private void HandleEventDropped()
        {
            Interlocked.Exchange(ref m_NumDroppedLogEvents, 0);
            ForwardInternalError("At least one logging event has been dropped to prevent buffer overflow. Suppressing this message for 30 seconds.", null, m_ThisType);
        }

        #endregion Forwarding

        #region Buffer Management

        private void SetBufferSize(int newBufferSize)
        {
            lock (m_BufferLock)
            {
                if (newBufferSize > 0 && newBufferSize != m_BufferSize)
                {
                    m_BufferSize = newBufferSize;
                    InitializeBuffer();
                }
            }
        }

        private void InitializeBuffer()
        {
            lock (m_BufferLock)
            {
                if (m_Buffer == null || m_Buffer.Size != m_BufferSize)
                {
                    m_Buffer = new ConcurrentFifoQueue<LoggingEventContext>(m_BufferSize);
                }
            }
        }

        #endregion Buffer Management
    }
}
