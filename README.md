# IOCPThreadPool
一个靠IOCP驱动的轻量线程池，支持自动扩容和收缩。

### 合理性
在 .net本身自带的线程池极其强大的今天是否还有必要搞这么个小玩意？

思前想后觉得还是有些场景下可以使用的，比如你就是不想只有这么全局一个线程池（clr的线程池是进程级别的，一个进程就一个）；又或者你想设置线程池的`concurrency level`而不仅仅是把这些一股脑甩给clr线程池。

### 简单说明
线程池本身比较简单，并没有复杂的调度算法而是另辟蹊径将这部分逻辑交由iocp来替我们完成。
两个完成端口分别用于：
- `dispatch`
  专门开了一个独立的线程用于dispatch，如此一来post操作本身将是非阻塞的了。
  另外，由于有这么一个额外的中间层存在，做池内部检测也比较好做了，线程池本身的自动收缩和扩容就是在这一层完成的
- `worker`
  完成端口有东西过来，worker拿到直接执行即可

### Experiment
项目中有一个`experiment`文件夹里面有几个很有意思的类型（从网上搜集而来）：
- `IOQueue`
  单线程处理管道
- `LimitedConcurrencyLevelTaskScheduler`
  限制了`concurrency level`的`TaskScheduler`
- `FixedThreadPoolScheduler`
  第二个类型的简化版本，从源码可以看出对gc不怎么友好，不过优势就在于简单易懂这在debug环境使用还是很方便的


