package com.byrsh.delaytask.handler;

import com.byrsh.delaytask.Task;
import com.byrsh.delaytask.TaskHandler;
import com.byrsh.delaytask.annotation.DelayTaskHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @Author: yangrusheng
 * @Description:
 * @Date: Created in 11:31 2019/8/15
 * @Modified By:
 */
@DelayTaskHandler(delayTaskZSetName = "delay-task")
public class SimpleHandler extends TaskHandler {

    private static final Logger LOGGER = LogManager.getLogger(SimpleHandler.class);

    @Override
    public void init() {
    }

    @Override
    public Boolean beforeExecute() {
        return true;
    }

    @Override
    public Boolean execute() {
        Task task = getTask();
        LOGGER.info("task ====== " + task.getContent() + " key: " + getKey());
        System.out.println("simpleHandler execute");
        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public void afterExecuted(Boolean result) {
        System.out.println(removeTask());
        System.out.println("simpleHandler task remove");
    }
}
