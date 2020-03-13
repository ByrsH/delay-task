package com.byrsh.delaytask.worker;


import com.byrsh.delaytask.Config;
import com.byrsh.delaytask.Task;
import com.byrsh.delaytask.TaskHandler;
import com.byrsh.delaytask.annotation.DelayTaskHandler;
import com.byrsh.delaytask.client.DelayTaskClient;
import com.byrsh.delaytask.util.JsonMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reflections.Reflections;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: yangrusheng
 * @Description:
 * @Date: Created in 15:05 2019/8/13
 * @Modified By:
 */
public class DefaultWorkerImpl implements Worker {

    private static final Logger LOGGER = LogManager.getLogger(DefaultWorkerImpl.class);

    private static final int CORE_POOL_SIZE = 4;
    private static final int MAX_POOL_SIZE = 8;
    private static final long KEEP_ALIVE_TIME = 60L;
    private static final int MAX_TASK_AMOUNT = CORE_POOL_SIZE * 20;

    private static final long WORKER_SLEEP_TIME = 10000L;
    private static final int TASK_NUMBER = 1;

    private final DelayTaskClient client;
    private final ExecutorService executor;
    private final ExecutorService checkExecutor;
    private final Map<String, Class> delayTaskHandlerMap = new ConcurrentHashMap<>();
    private final List<String> keys = new CopyOnWriteArrayList<>();
    private final AtomicInteger sign = new AtomicInteger(0);

    /**
     * 延迟任务处理线程池核心线程数
     */
    private int corePoolSize = CORE_POOL_SIZE;

    /**
     * 延迟任务处理线程池最大线程数
     */
    private int maxPoolSize = MAX_POOL_SIZE;

    /**
     * 空闲线程存活时间, 单位：s
     */
    private long keepAliveTime = KEEP_ALIVE_TIME;

    /**
     * 线程池最大处理任务数
     */
    private int maxTaskAmount = MAX_TASK_AMOUNT;

    /**
     * worker 线程从redis获取task的间隔时间
     */
    private long workerSleepTime = WORKER_SLEEP_TIME;

    /**
     * 每次从redis获取任务数
     */
    private int taskNumber = TASK_NUMBER;

    /**
     * 检查线程间隔执行时间
     */
    private long checkoutSleepTime;

    /**
     * 任务执行超时时间，超过将重新放入待执行集合
     */
    private long executeTimeoutTime;

    public DefaultWorkerImpl(DelayTaskClient client, Config config) {
        if (client == null) {
            throw new IllegalArgumentException("delayTaskClient must not be null");
        } else {
            this.client = client;
        }
        if (config == null || config.getScanPackageName() == null) {
            throw new IllegalArgumentException("config must not be null, it must have scanPackageName value at least");
        }
        //扫描指定文件夹，获取实现了TaskHandler的类信息
        scanDelayTaskHandler(config.getScanPackageName());

        //设置任务执行流量池相关的属性
        if (config.getCorePoolSize() > 0) {
            this.corePoolSize = config.getCorePoolSize();
        }
        if (config.getMaxPoolSize() > 0) {
            this.maxPoolSize = config.getMaxPoolSize();
        }
        if (config.getKeepAliveTime() > 0L) {
            this.keepAliveTime = config.getKeepAliveTime();
        }
        if (config.getMaxTaskAmount() > 0) {
            this.maxTaskAmount = config.getMaxTaskAmount();
        }
        if (config.getTaskNumber() > 1) {
            this.taskNumber = config.getTaskNumber();
        }
        if (config.getCheckoutSleepTime() > 0L) {
            this.checkoutSleepTime = config.getCheckoutSleepTime();
        }
        if (config.getExecuteTimeoutTime() > 0L) {
            this.executeTimeoutTime = config.getExecuteTimeoutTime();
        }

        this.executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(maxTaskAmount), new DelayTaskThreadFactory());

        //检查线程
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("delay-task-check-pool-%d").build();
        this.checkExecutor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(1024), namedThreadFactory,
                new ThreadPoolExecutor.AbortPolicy());
    }

    private void scanDelayTaskHandler(String scanPackageName) {
        Reflections reflections = new Reflections(scanPackageName);
        Set<Class<?>> annotatedClasses = reflections.getTypesAnnotatedWith(DelayTaskHandler.class);
        for (Class<?> classObj: annotatedClasses) {
            if (TaskHandler.class.isAssignableFrom(classObj)) {
                DelayTaskHandler handler = classObj.getAnnotation(DelayTaskHandler.class);
                delayTaskHandlerMap.put(handler.delayTaskZSetName(), classObj);
                keys.add(handler.delayTaskZSetName());
            }
        }
        if (delayTaskHandlerMap.size() == 0) {
            throw new RuntimeException("delayTask application not implements taskHandler or not use DelayTaskHandler"
                    + " annotation.");
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                //sleep 时间间隔，放在前面是为了防止 handle 类初始化时某些属性获取不到。
                Thread.sleep(workerSleepTime);

                // 循环具体延迟任务处理类，从 redis 获取任务执行
                for (Map.Entry<String, Class> entry : delayTaskHandlerMap.entrySet()) {
                    String key = entry.getKey();
                    Class handlerClass = entry.getValue();
                    List<Task> tasks = client.popTasks(key, taskNumber);
                    if (tasks != null && !tasks.isEmpty()) {
                        handlerTasks(tasks, handlerClass, key);
                    }
                }

                //启动检查线程，把执行超时的任务重新放入待执行集合中
                if (sign.intValue() == 0) {
                    CheckoutWorker checkoutWorker = new DefaultCheckoutWorkerImpl(client, keys, checkoutSleepTime,
                            executeTimeoutTime, taskNumber);
                    checkExecutor.execute(checkoutWorker);
                    LOGGER.info("checkoutWorker thread start.");
                    sign.incrementAndGet();
                }
            } catch (Exception e) {
                LOGGER.error("defaultWorkerImpl thread run Exception.", e);
            }
        }
    }

    @Override
    public void destroy() {
        executor.shutdown();
        checkExecutor.shutdown();
    }

    private void handlerTasks(List<Task> tasks, Class handlerClass, String key) {
        for (Task task: tasks) {
            try {
                if (!isExecuteTask(task)) {
                    //删除过期任务
                    client.removeExpireTask(task, key);
                    LOGGER.info("delay task:{} expire, so remove task",
                            JsonMapper.writeValueAsString(task));
                    continue;
                }
                TaskHandler handler = (TaskHandler) handlerClass.newInstance();
                handler.setClient(client);
                handler.setKey(key);
                handler.setTask(task);
                FutureTask<Boolean> delayHandlerTask = new FutureTask<>(handler);
                executor.submit(delayHandlerTask);
            } catch (RejectedExecutionException e) {
                LOGGER.error("handlerClass: {} executor submit queue is full, Exception: {}", handlerClass.getName(),
                        e.toString());
                // 线程池任务队列已满的情况下，重新放入redis 延时任务集合
                client.reAddTask(task, key);
            } catch (InstantiationException e) {
                LOGGER.error("handlerClass: {} new instance Exception: {}", handlerClass.getName(), e.toString());
            } catch (IllegalAccessException e) {
                LOGGER.error("handlerClass: {} new instance Exception: {}", handlerClass.getName(), e.toString());
            } catch (Exception e) {
                LOGGER.error("handler: {} execute delay task exception: {}", handlerClass.getName(), e.toString());
            }
        }
    }

    private boolean isExecuteTask(Task task) {
        if (task.getExpireTime() == null || task.getExpireTime().equals(0L)) {
            return true;
        }
        if (task.getExpireTime().compareTo(System.currentTimeMillis()) < 0) {
            return false;
        }
        return true;
    }

}
