package com.byrsh.delaytask.util;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: yangrusheng
 * @Description: 控制 worker 线程不取任务
 * @Date: Created in 0:21 2020/3/22
 * @Modified By:
 */
public class WorkerControl {

    private static final AtomicBoolean execute = new AtomicBoolean(true);

    public static boolean isExecute() {
        if (execute.get()) {
            return true;
        } else {
            executeWait();
            return false;
        }
    }

    public static void setExecute(boolean execute) {
        synchronized (WorkerControl.execute) {
            WorkerControl.execute.set(execute);
            if (execute) {
                WorkerControl.execute.notifyAll();
            }
        }
    }

    public static void executeWait() {
        synchronized (WorkerControl.execute) {
            try {
                WorkerControl.execute.wait();
            } catch (Exception e) {
                //e.printStackTrace();
            }
        }
    }

}
