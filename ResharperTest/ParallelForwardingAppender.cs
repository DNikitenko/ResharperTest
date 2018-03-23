namespace Log4Net.Async
{
    using log4net.Core;
    using log4net.Util;
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// An asynchronous appender based on <see cref="BlockingCollection'T"/>
    /// </summary>
    public class ParallelForwardingAppender : AsyncForwardingAppenderBase, IDisposable
    {
        #region Private Members

        private const int DEFAULT_BUFFER_SIZE = 1000;
        private BlockingCollection<LoggingEventContext> m_LoggingEvents;
        private CancellationTokenSource m_LoggingCancelationTokenSource;
        private CancellationToken m_LoggingCancelationToken;
        private Task m_LoggingTask;
        private double m_ShutdownFlushTimeout = 5;
        private static readonly Type m_ThisType = typeof(ParallelForwardingAppender);
        private volatile bool m_ShutDownRequested;
        private int m_BufferSize = DEFAULT_BUFFER_SIZE;

        #endregion Private Members

        #region Properties

        /// <summary>
        /// Gets or sets the number of LoggingEvents that will be buffered.  Set to null for unlimited.
        /// </summary>
        public override int BufferSize
        {
            get { return m_BufferSize; }
            set { m_BufferSize = value; }
        }

        public int BufferEntryCount
        {
            get
            {
                if (m_LoggingEvents == null) return 0;
                return m_LoggingEvents.Count;
            }
        }

        /// <summary>
        /// Gets or sets the time period in which the system will wait for appenders to flush before canceling the background task.
        /// </summary>
        public double ShutdownFlushTimeout
        {
            get
            {
                return m_ShutdownFlushTimeout;
            }
            set
            {
                m_ShutdownFlushTimeout = value;
            }
        }

        protected override string InternalLoggerName
        {
            get
            {
                {
                    return "ParallelForwardingAppender";
                }
            }
        }

        #endregion Properties

        #region Startup

        public override void ActivateOptions()
        {
            base.ActivateOptions();
            StartForwarding();
        }

        private void StartForwarding()
        {
            if (m_ShutDownRequested)
            {
                return;
            }
            //Create a collection which will block the thread and wait for new entries
            //if the collection is empty
            if (BufferSize > 0)
            {
                m_LoggingEvents = new BlockingCollection<LoggingEventContext>(BufferSize);
            }
            else
            {
                //No limit on the number of events.
                m_LoggingEvents = new BlockingCollection<LoggingEventContext>();
            }
            //The cancellation token is used to cancel a running task gracefully.
            m_LoggingCancelationTokenSource = new CancellationTokenSource();
            m_LoggingCancelationToken = m_LoggingCancelationTokenSource.Token;
            m_LoggingTask = new Task(SubscriberLoop, m_LoggingCancelationToken);
            m_LoggingTask.Start();
        }

        #endregion Startup

        #region Shutdown

        private void CompleteSubscriberTask()
        {
            m_ShutDownRequested = true;
            if (m_LoggingEvents == null || m_LoggingEvents.IsAddingCompleted)
            {
                return;
            }
            //Don't allow more entries to be added.
            m_LoggingEvents.CompleteAdding();

            //Allow some time to flush
            var sleepInterval = TimeSpan.FromMilliseconds(100);
            var flushTimespan = TimeSpan.FromSeconds(m_ShutdownFlushTimeout);

            //Sleep until either timeout is expired or all events have flushed
            while (flushTimespan >= sleepInterval && !m_LoggingEvents.IsCompleted)
            {
                flushTimespan -= sleepInterval;
                Thread.Sleep(sleepInterval);
            }

            if (!m_LoggingTask.IsCompleted && !m_LoggingCancelationToken.IsCancellationRequested)
            {
                m_LoggingCancelationTokenSource.Cancel();
                //Wait here so that the error logging messages do not get into a random order.
                //Don't pass the cancellation token because we are not interested
                //in catching the OperationCanceledException that results.
                m_LoggingTask.Wait();
            }
            if (!m_LoggingEvents.IsCompleted)
            {
                ForwardInternalError("The buffer was not able to be flushed before timeout occurred.", null, m_ThisType);
            }
        }

        protected override void OnClose()
        {
            CompleteSubscriberTask();
            base.OnClose();
        }

        #endregion Shutdown

        #region Appending

        protected override void Append(LoggingEvent loggingEvent)
        {
            if (m_LoggingEvents == null || m_LoggingEvents.IsAddingCompleted || loggingEvent == null)
            {
                return;
            }

            loggingEvent.Fix = Fix;
            //In the case where blocking on a full collection, and the task is subsequently completed, the cancellation token
            //will prevent the entry from attempting to add to the completed collection which would result in an exception.
            m_LoggingEvents.Add(new LoggingEventContext(loggingEvent, HttpContext), m_LoggingCancelationToken);
        }

        protected override void Append(LoggingEvent[] loggingEvents)
        {
            if (m_LoggingEvents == null || m_LoggingEvents.IsAddingCompleted || loggingEvents == null)
            {
                return;
            }

            foreach (var loggingEvent in loggingEvents)
            {
                Append(loggingEvent);
            }
        }

        #endregion Appending

        #region Forwarding

        /// <summary>
        /// Iterates over a BlockingCollection containing LoggingEvents.
        /// </summary>
        private void SubscriberLoop()
        {
            Thread.CurrentThread.Name = string.Format("{0} ParallelForwardingAppender Subscriber Task", Name);
            //The task will continue in a blocking loop until
            //the queue is marked as adding completed, or the task is canceled.
            try
            {
                //This call blocks until an item is available or until adding is completed
                foreach (var entry in m_LoggingEvents.GetConsumingEnumerable(m_LoggingCancelationToken))
                {
                    HttpContext = entry.HttpContext;
                    ForwardLoggingEvent(entry.LoggingEvent, m_ThisType);
                }
            }
            catch (OperationCanceledException ex)
            {
                //This is to avoid throwing an exception when there is no event
                if (m_LoggingEvents.Count == 0) return;

                //The thread was canceled before all entries could be forwarded and the collection completed.
                ForwardInternalError("Subscriber task was canceled before completion.", ex, m_ThisType);
                //Cancellation is called in the CompleteSubscriberTask so don't call that again.
            }
            catch (ThreadAbortException ex)
            {
                //Thread abort may occur on domain unload.
                ForwardInternalError("Subscriber task was aborted.", ex, m_ThisType);
                //Cannot recover from a thread abort so complete the task.
                CompleteSubscriberTask();
                //The exception is swallowed because we don't want the client application
                //to halt due to a logging issue.
            }
            catch (Exception ex)
            {
                //On exception, try to log the exception
                ForwardInternalError("Subscriber task error in forwarding loop.", ex, m_ThisType);
                //Any error in the loop is going to be some sort of extenuating circumstance from which we
                //probably cannot recover anyway.   Complete subscribing.
                CompleteSubscriberTask();
            }
        }

        #endregion Forwarding

        #region IDisposable Implementation

        private bool m_Disposed;

        //Implement IDisposable.
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!m_Disposed)
            {
                if (disposing)
                {
                    if (m_LoggingTask != null)
                    {
                        if (!(m_LoggingTask.IsCanceled || m_LoggingTask.IsCompleted || m_LoggingTask.IsFaulted))
                        {
                            try
                            {
                                CompleteSubscriberTask();
                            }
                            catch (Exception ex)
                            {
                                LogLog.Error(m_ThisType, "Exception Completing Subscriber Task in Dispose Method", ex);
                            }
                        }
                        try
                        {
                            m_LoggingTask.Dispose();
                        }
                        catch (Exception ex)
                        {
                            LogLog.Error(m_ThisType, "Exception Disposing Logging Task", ex);
                        }
                        finally
                        {
                            m_LoggingTask = null;
                        }
                    }
                    if (m_LoggingEvents != null)
                    {
                        try
                        {
                            m_LoggingEvents.Dispose();
                        }
                        catch (Exception ex)
                        {
                            LogLog.Error(m_ThisType, "Exception Disposing BlockingCollection", ex);
                        }
                        finally
                        {
                            m_LoggingEvents = null;
                        }
                    }
                    if (m_LoggingCancelationTokenSource != null)
                    {
                        try
                        {
                            m_LoggingCancelationTokenSource.Dispose();
                        }
                        catch (Exception ex)
                        {
                            LogLog.Error(m_ThisType, "Exception Disposing CancellationTokenSource", ex);
                        }
                        finally
                        {
                            m_LoggingCancelationTokenSource = null;
                        }
                    }
                }
                m_Disposed = true;
            }
        }

        // Use C# destructor syntax for finalization code.
        ~ParallelForwardingAppender()
        {
            // Simply call Dispose(false).
            Dispose(false);
        }

        #endregion IDisposable Implementation
    }
}
