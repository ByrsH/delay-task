package com.byrsh.delaytask;

import com.byrsh.delaytask.client.DelayTaskClient;
import com.byrsh.delaytask.worker.DefaultWorkerImpl;
import com.byrsh.delaytask.worker.Worker;

/**
 * @Author: yangrusheng
 * @Description:
 * @Date: Created in 11:29 2019/8/15
 * @Modified By:
 */
public class DelayTaskTest {

    public static void main(String[] args) {
        Long time = System.currentTimeMillis();
        Double timedouble = time.doubleValue();
        System.out.println(time.compareTo(timedouble.longValue()));
        System.out.println(time + " ===== " + time.doubleValue());
        Config config = new Config();
        config.setRedisHost("127.0.0.1");
        config.setRedisPort(6379);
        config.setScanPackageName("com.byrsh.delaytask.handler");

        DelayTaskClient client = new DelayTaskClient(config);

        Task task = new Task();
        task.setContent("test");
        task.setCreateTime(System.currentTimeMillis());
        task.setDelayTime(System.currentTimeMillis());
        task.setExpireTime(0L);
        client.addTask(task, "delay-task");

//        List<Task> tasks = client.popTasks("delay-task", 0);
//        System.out.println("=========" + client.reAddTimeoutHandlerTask("delay-task", 1, 1000));
//        System.out.println("tasks size : " + tasks.size());

        Worker worker = new DefaultWorkerImpl(client, config);
        Thread thread = new Thread(worker);
        thread.start();
    }
}
