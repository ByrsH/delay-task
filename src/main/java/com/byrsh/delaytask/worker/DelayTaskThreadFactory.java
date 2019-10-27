package com.byrsh.delaytask.worker;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: yangrusheng
 * @Description: copy 默认的factory，改了下线程名，方便问题追溯
 * @Date: Created in 17:55 2019/8/14
 * @Modified By:
 */
public class DelayTaskThreadFactory implements ThreadFactory {
    private static final AtomicInteger poolNumber = new AtomicInteger(1);
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    DelayTaskThreadFactory() {
        SecurityManager var1 = System.getSecurityManager();
        this.group = var1 != null ? var1.getThreadGroup() : Thread.currentThread().getThreadGroup();
        this.namePrefix = "delay-task-pool-" + poolNumber.getAndIncrement() + "-thread-";
    }

    @Override
    public Thread newThread(Runnable var1) {
        Thread var2 = new Thread(this.group, var1, this.namePrefix + this.threadNumber.getAndIncrement(), 0L);
        if (var2.isDaemon()) {
            var2.setDaemon(false);
        }

        if (var2.getPriority() != 5) {
            var2.setPriority(5);
        }

        return var2;
    }
}
