using Microsoft.Win32.SafeHandles;
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Threading;

namespace ThePool
{
    /// <summary> 
    /// 一个靠IOCP驱动的轻量线程池，支持自动扩容和收缩
    /// </summary>
    public class IOCPThreadPool : IDisposable
    {
        private struct Work
        {
            public Action<object> Callback;
            public object State;
        }

        #region Win32 API
        /// <summary> 
        /// 关闭IOCP线程池
        /// </summary>
        [DllImport("Kernel32", CharSet = CharSet.Auto)]
        private static extern bool CloseHandle(SafeHandle hObject);

        /// <summary>
        /// Creates an input/output (I/O) completion port and associates it with a specified file handle, 
        /// or creates an I/O completion port that is not yet associated with a file handle, allowing association at a later time.
        /// </summary>
        /// <param name="fileHandle">An open file handle or INVALID_HANDLE_VALUE.</param>
        /// <param name="existingCompletionPort">A handle to an existing I/O completion port or NULL.</param>
        /// <param name="completionKey">The per-handle user-defined completion key that is included in every I/O completion packet for the specified file handle.</param>
        /// <param name="numberOfConcurrentThreads">The maximum number of threads that the operating system can allow to concurrently process I/O completion packets for the I/O completion port.</param>
        /// <returns>If the function succeeds, the return value is the handle to an I/O completion port.  If the function fails, the return value is NULL.</returns>
        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        private static extern SafeFileHandle CreateIoCompletionPort(IntPtr fileHandle, IntPtr existingCompletionPort, UIntPtr completionKey, uint numberOfConcurrentThreads);

        /// <summary>Posts an I/O completion packet to an I/O completion port.</summary>
        /// <param name="completionPort">A handle to the completion port.</param>
        /// <param name="dwNumberOfBytesTransferred">The value to be returned through the lpNumberOfBytesTransferred parameter of the GetQueuedCompletionStatus function.</param>
        /// <param name="dwCompletionKey">The value to be returned through the lpCompletionKey parameter of the GetQueuedCompletionStatus function.</param>
        /// <param name="lpOverlapped">The value to be returned through the lpOverlapped parameter of the GetQueuedCompletionStatus function.</param>
        /// <returns>If the function succeeds, the return value is nonzero. If the function fails, the return value is zero.</returns>
        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        private static extern bool PostQueuedCompletionStatus(SafeFileHandle completionPort, IntPtr dwNumberOfBytesTransferred, IntPtr dwCompletionKey, IntPtr lpOverlapped);

        /// <summary>Attempts to dequeue an I/O completion packet from the specified I/O completion port.</summary>
        /// <param name="completionPort">A handle to the completion port.</param>
        /// <param name="lpNumberOfBytes">A pointer to a variable that receives the number of bytes transferred during an I/O operation that has completed.</param>
        /// <param name="lpCompletionKey">A pointer to a variable that receives the completion key value associated with the file handle whose I/O operation has completed.</param>
        /// <param name="lpOverlapped">A pointer to a variable that receives the address of the OVERLAPPED structure that was specified when the completed I/O operation was started.</param>
        /// <param name="dwMilliseconds">The number of milliseconds that the caller is willing to wait for a completion packet to appear at the completion port. </param>
        /// <returns>Returns nonzero (TRUE) if successful or zero (FALSE) otherwise.</returns>
        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        private static extern bool GetQueuedCompletionStatus(IntPtr completionPort, out uint lpNumberOfBytes, out IntPtr lpCompletionKey, out IntPtr lpOverlapped, uint dwMilliseconds);

        /// <summary> 
        /// 表示Win32 An invalid file handle value
        /// </summary>
        private readonly IntPtr INVALID_FILE_HANDLE = unchecked((IntPtr)(-1));

        /// <summary> 
        /// 表示Win32 invalid I/O completion port handle value
        /// </summary>
        private readonly IntPtr INVALID_IOCP_HANDLE = IntPtr.Zero;

        /// <summary> 
        /// 表示需要关闭IOCP worker thread
        /// </summary>
        private readonly IntPtr SHUTDOWN_IOCPTHREAD = new IntPtr(0x7fffffff);

        /// <summary> 
        /// 表示Win32 INFINITE Macro 
        /// </summary>
        private readonly uint INFINITE_TIMEOUT = unchecked((uint)Timeout.Infinite);

        /// <summary> 
        /// 表示等待操作超时
        /// </summary>
        private readonly uint WAIT_TIMEOUT = 0x102;
        #endregion

        #region 私有字段
        // 锁对象
        private object _lockobj;
        // IOCP线程池DispatchHandle
        private SafeFileHandle _dispatchHandle;
        // IOCP线程池WorkHandle
        private SafeFileHandle _workHandle;
        // IOCP线程池中允许的最大同时运行线程数
        private int _maxConcurrency;
        // IOCP线程池中维护的最小线程数
        private int _minThreads;
        // IOCP线程池中维护的最大线程数
        private int _maxThreads;
        // IOCP线程池中允许的最大空闲线程数
        private int _maxIdleThreads;
        // IOCP线程池中当前线程计数
        private int _currentThreads;
        // IOCP线程池中活动线程计数
        private int _activeThreads;
        // IOCP线程池当前work项计数
        private int _currentWorks88;
        // IOCP线程池维护刷新周期（单位：毫秒）
        private int _poolMaintPeriod;
        // Dispatch请求超时时间（单位：毫秒）
        private int _dispatchTimeout;
        // 线程池满状态运行时允许的Dispatch请求超时时间（单位：毫秒）
        private int _maxThreadsDispatchTimeout;
        // 用户委托
        private Action<object> _userFunction;
        // IOCP线程池是否已被dispose
        private bool _disposed;
        // 关闭事件
        private ManualResetEventSlim _shutdownEvent;
        // dispatch完毕事件
        private ManualResetEventSlim _dispatchCompleteEvent;
        #endregion

        #region 公共属性
        /// <summary>
        /// IOCP线程池中当前线程计数
        /// </summary>
        public int CurrentThreads { get { return _currentThreads; } }

        /// <summary>
        /// IOCP线程池中活动线程计数
        /// </summary>
        public int ActiveThreads { get { return _activeThreads; } }
        #endregion

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="maxConcurrency">IOCP线程池最大并发线程数</param>
        /// <param name="minThreadsInPool">IOCP线程池最小维护线程数</param>
        /// <param name="maxThreadsInPool">IOCP线程池最大维护线程数</param>
        /// <param name="userFunction">用户委托</param>
        public IOCPThreadPool(int maxConcurrency, int minThreadsInPool, int maxThreadsInPool, Action<object> userFunction)
        {
            _maxConcurrency = maxConcurrency;
            _minThreads = minThreadsInPool;
            _maxThreads = maxThreadsInPool;
            _userFunction = userFunction;
            _maxIdleThreads = 0;
            _currentThreads = 0;
            _activeThreads = 0;
            _poolMaintPeriod = 1000 * 5;
            _dispatchTimeout = 100;
            _maxThreadsDispatchTimeout = 1000 * 10;
            _shutdownEvent = new ManualResetEventSlim(false);
            _dispatchCompleteEvent = new ManualResetEventSlim(false);
            _lockobj = new object();
            _disposed = false;

            InitDispatchIOCP();
            InitWorkIOCP();
        }

        /// <summary>
        /// post请求到IOCP线程池
        /// </summary>
        /// <param name="value">传递给委托的参数</param>
        public void PostEvent(object state)
        {
            // 仅当IOCP线程没有被销毁时执行
            if (_disposed == false)
            {
                GCHandle handle = GCHandle.Alloc(state, GCHandleType.Pinned);
                unsafe
                {
                    // post请求
                    PostQueuedCompletionStatus(
                        _dispatchHandle,
                        IntPtr.Zero,
                        IntPtr.Zero,
                        handle.AddrOfPinnedObject());
                }
            }
        }

        /// <summary>
        /// post请求到IOCP线程池
        /// </summary>
        public void PostEvent()
        {
            // 仅当IOCP线程没有被销毁时执行
            if (_disposed == false)
            {
                unsafe
                {
                    // post请求
                    PostQueuedCompletionStatus(_dispatchHandle, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
                }
            }
        }

        public void ShutDown()
        {
            Dispose();
        }

        /// <summary>
        /// 初始化dispatch loop
        /// </summary>
        private void InitDispatchIOCP()
        {
            unsafe
            {
                _dispatchHandle = CreateIoCompletionPort(INVALID_FILE_HANDLE, INVALID_IOCP_HANDLE, UIntPtr.Zero, (uint)1);
            }
            if (_dispatchHandle.IsInvalid)
                throw new Exception("创建Dispatch IOCP失败");

            CreateAndStartDispatchThread();
        }

        /// <summary>
        /// 初始化work loop
        /// </summary>
        private void InitWorkIOCP()
        {
            unsafe
            {
                _workHandle = CreateIoCompletionPort(INVALID_FILE_HANDLE, INVALID_IOCP_HANDLE, UIntPtr.Zero, (uint)_maxThreads);
            }
            if (_workHandle.IsInvalid)
                throw new Exception("创建Work IOCP失败");

            var count = _minThreads;
            for (var i = 0; i < count; i++)
            {
                CreateAndStartWorkThread();
                IncCurrentThreads();
            }
        }

        /// <summary>
        /// 创建并开启dispatch thread
        /// </summary>
        private void CreateAndStartDispatchThread()
        {
            var thread = new Thread(DispatchLoop);
            thread.Name = "IOCP Dispatch Thread[{0}]";
            thread.IsBackground = true;
            thread.Start();
        }

        /// <summary>
        /// 创建并开启work thread
        /// </summary>
        private void CreateAndStartWorkThread()
        {
            var thread = new Thread(WorkLoop);
            thread.Name = string.Format("IOCP Work Thread[{0}]", thread.GetHashCode());
            thread.IsBackground = true;
            thread.Start();
        }

        /// <summary>
        /// dispatch loop
        /// </summary>
        private void DispatchLoop()
        {
            uint lpNumberOfBytes;
            IntPtr lpCompletionKey;
            IntPtr lpOverlapped;
            try
            {
                var lastMaintenance = DateTime.Now.Ticks;
                while (true)
                {
                    if (_shutdownEvent.Wait(0)) // 仅检查标志，立即返回
                    {
                        // 关闭事件触发，退出loop
                        break;
                    }

                    var ret = false;
                    unsafe
                    {
                        // 等待IO完成
                        ret = GetQueuedCompletionStatus(
                            _dispatchHandle.DangerousGetHandle(),
                            out lpNumberOfBytes,
                            out lpCompletionKey,
                            out lpOverlapped,
                            (uint)_dispatchTimeout);
                    }

                    // 再检查一次
                    if (_shutdownEvent.Wait(0))
                    {
                        break;
                    }

                    if (ret)
                    {
                        InnerDispatch(lpOverlapped);

                        if (DateTime.Now.Ticks - lastMaintenance >= _poolMaintPeriod)
                        {
                            HandleIdleThreads();

                            lastMaintenance = DateTime.Now.Ticks;
                        }
                    }
                    else
                    {
                        var lastError = Marshal.GetLastWin32Error();
                        if (lastError == WAIT_TIMEOUT)
                        {
                            HandleIdleThreads();
                        }
                        else
                        {
                            throw new Win32Exception(lastError);
                        }
                    }
                }
            }
            catch
            {
            }
        }

        /// <summary>
        /// InnerDispatch
        /// </summary>
        private void InnerDispatch(IntPtr lpOverlapped)
        {
            var wait_arrs = new WaitHandle[] { _dispatchCompleteEvent.WaitHandle, _shutdownEvent.WaitHandle };
            _dispatchCompleteEvent.Reset();

            var processed = false;

            QueueWorkItem(lpOverlapped);

            var timeout = _dispatchTimeout + (_currentThreads == _maxThreads ? _maxThreadsDispatchTimeout : 0);
            while (!processed)
            {
                var ret = WaitHandle.WaitAny(wait_arrs, timeout); // 返回的是数组索引
                // 成功派发到工作线程池
                if (ret == 0)
                {
                    processed = true;
                }
                // 关闭事件触发
                else if (ret == 1)
                {
                    break;
                }
                // 等待超时
                else if (ret == WAIT_TIMEOUT)
                {
                    if (_currentThreads < _maxThreads &&
                        _activeThreads == _currentThreads)
                    {
                        // 创建并开启一个新的线程
                        CreateAndStartWorkThread();
                        IncCurrentThreads();
                    }
                }
                else
                {
                    throw new Exception("未知系统错误");
                }
            }
        }

        /// <summary>
        /// work loop
        /// </summary>
        private void WorkLoop()
        {
            uint numbserOfBytes;
            IntPtr lpCompletionKey;
            IntPtr lpOverlapped;
            try
            {
                while (true)
                {
                    unsafe
                    {
                        // 等待IO完成
                        GetQueuedCompletionStatus(
                            _workHandle.DangerousGetHandle(),
                            out numbserOfBytes,
                            out lpCompletionKey,
                            out lpOverlapped,
                            INFINITE_TIMEOUT);
                    }

                    if (numbserOfBytes <= 0)
                        continue;

                    // 如果请求的是关闭
                    if (lpCompletionKey == SHUTDOWN_IOCPTHREAD)
                        break;

                    // 当前活动线程计数加1
                    IncActiveThreads();
                    try
                    {
                        // 调用用户指定的委托
                        GCHandle handle = GCHandle.FromIntPtr(lpOverlapped);
                        object state = handle.Target;
                        _userFunction(state);
                    }
                    catch
                    {
                    }

                    // 用户委托执行完毕，当前活动线程计数减1
                    DecActiveThreads();
                }
            }
            catch
            {
            }

            // 收到退出请求，当前线程计数减1
            DecCurrentThreads();
        }

        /// <summary>
        /// post请求到工作线程
        /// </summary>
        private void QueueWorkItem(IntPtr lpOverlapped)
        {
            unsafe
            {
                PostQueuedCompletionStatus(_workHandle, IntPtr.Zero, IntPtr.Zero, lpOverlapped);
            }
        }

        /// <summary>
        /// 处理空闲线程，关闭它们
        /// </summary>
        private void HandleIdleThreads()
        {
            if (_activeThreads > _minThreads)
            {
                var idelThreads = _currentThreads - _activeThreads;

                if (idelThreads > _maxIdleThreads)
                {
                    var threadsToShutdown = (idelThreads - _maxIdleThreads) / 2 + 1;

                    StopWorkThread(threadsToShutdown);
                }
            }
        }

        /// <summary>
        /// 关闭工作线程
        /// </summary>
        private void StopWorkThread(int count)
        {
            // post关闭请求到工作线程池
            unsafe
            {
                for (int i = 0; i < count; i++)
                    PostQueuedCompletionStatus(_workHandle, IntPtr.Zero, SHUTDOWN_IOCPTHREAD, IntPtr.Zero);
            }
        }

        /// <summary>
        /// 析构函数
        /// </summary>
        ~IOCPThreadPool()
        {
            if (!_disposed)
                Dispose();
        }

        /// <summary>
        /// 关闭IOCP线程池，销毁相关资源
        /// </summary>
        public void Dispose()
        {
            try
            {
                _disposed = true;
                _shutdownEvent.Set();

                var count = _currentThreads;
                /*
                 * The GetQueuedCompletionStatus Win32 API method will cause the thread to run outside the 
                 * scope of the CLR and the .NET framework will lose access to the thread. So what we do is 
                 * post work into the IOCP thread pool. 
                 */
                StopWorkThread(count);

                // 等待线程计数归零
                while (_currentThreads != 0) Thread.Sleep(100);

                _dispatchCompleteEvent.Dispose();
                _shutdownEvent.Dispose();
                unsafe
                {
                    // 释放非托管资源
                    CloseHandle(_dispatchHandle);
                    CloseHandle(_workHandle);
                }
            }
            catch
            {
            }
        }

        #region 辅助方法
        /// <summary>
        ///  增加IOCP线程池当前线程计数
        /// </summary>
        private int IncCurrentThreads()
        {
            return Interlocked.Increment(ref _currentThreads);
        }

        /// <summary>
        ///  减少IOCP线程池当前线程计数
        /// </summary>
        private int DecCurrentThreads()
        {
            return Interlocked.Decrement(ref _currentThreads);
        }

        /// <summary>
        /// 增加IOCP线程池活动线程计数
        /// </summary>
        private int IncActiveThreads()
        {
            return Interlocked.Increment(ref _activeThreads);
        }

        /// <summary>
        /// 减少IOCP线程池活动线程计数
        /// </summary>
        private int DecActiveThreads()
        {
            return Interlocked.Decrement(ref _activeThreads);
        }
        #endregion
    }
}
