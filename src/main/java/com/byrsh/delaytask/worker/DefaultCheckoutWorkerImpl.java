package com.byrsh.delaytask.worker;

import com.byrsh.delaytask.client.DelayTaskClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * @Author: yangrusheng
 * @Description:
 * @Date: Created in 9:40 2019/8/14
 * @Modified By:
 */
public class DefaultCheckoutWorkerImpl implements CheckoutWorker {

    private static final Logger LOGGER = LogManager.getLogger(DefaultCheckoutWorkerImpl.class);

    private static final long CHECKOUT_SLEEP_TIME = 60000L;
    private static final long EXECUTE_TIMEOUT_TIME = 60000L;

    private DelayTaskClient client;

    /**
     * zset key 列表
     */
    private List<String> keys;

    /**
     * 检查线程间隔执行时间，单位：ms
     */
    private long checkoutSleepTime = CHECKOUT_SLEEP_TIME;

    /**
     * 任务执行超时时间，超过将重新放入待执行集合，单位：ms
     */
    private long executeTimeoutTime = EXECUTE_TIMEOUT_TIME;

    private final int reAddTimeoutTaskNumber;

    public DefaultCheckoutWorkerImpl(DelayTaskClient client, List<String> keys, long checkoutSleepTime,
                                     long executeTimeoutTime, int taskNumber) {
        this.client = client;
        this.keys = keys;
        reAddTimeoutTaskNumber = taskNumber * 3;
        if (checkoutSleepTime > 0L) {
            this.checkoutSleepTime = checkoutSleepTime;
        }
        if (executeTimeoutTime > 0L) {
            this.executeTimeoutTime = executeTimeoutTime;
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(checkoutSleepTime);
                for (String key : keys) {
                    client.reAddTimeoutHandlerTask(key, reAddTimeoutTaskNumber, executeTimeoutTime);
                }
            } catch (Exception e) {
                LOGGER.error("checkout worker exception: ", e);
            }
        }
    }
}
