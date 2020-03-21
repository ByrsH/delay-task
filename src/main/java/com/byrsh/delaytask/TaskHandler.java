package com.byrsh.delaytask;

import com.byrsh.delaytask.client.DelayTaskClient;

import java.util.concurrent.Callable;

/**
 * @Author: yangrusheng
 * @Description:
 * @Date: Created in 14:35 2019/8/13
 * @Modified By:
 */
public abstract class TaskHandler implements Callable<Boolean> {

    /**
     * 任务
     */
    private Task task;

    /**
     * redis zset key
     */
    private String key;

    private DelayTaskClient client;

    @Override
    public Boolean call() throws Exception {
        init();
        boolean before = beforeExecute();
        if (!before) {
            return false;
        }
        Boolean result = execute();
        afterExecuted(result);
        return result;
    }

    /**
     * 初始化一些成员变量
     */
    public abstract void init();

    /**
     * 任务执行前操作
     * @return 是否继续执行
     */
    public abstract Boolean beforeExecute();

    /**
     * 任务执行
     * @return  执行结果
     */
    public abstract Boolean execute();

    /**
     * 任务执行完成后的处理，例如删除已经执行过的任务
     * @param result  结果
     */
    public abstract void afterExecuted(Boolean result);

    /**
     * 移除任务
     * @return  结果
     */
    public boolean removeTask() {
        return client.removeExecutedTask(task, key);
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    public DelayTaskClient getClient() {
        return client;
    }

    public void setClient(DelayTaskClient client) {
        this.client = client;
    }

}
