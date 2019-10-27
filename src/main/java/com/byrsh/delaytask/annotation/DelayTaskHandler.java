package com.byrsh.delaytask.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @Author: yangrusheng
 * @Description: 延迟任务处理注解，用于延迟任务业务处理的注解。
 * @Date: Created in 15:59 2019/8/13
 * @Modified By:
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DelayTaskHandler {
    /**
     * 任务所在集合名称
     * @return
     */
    String delayTaskZSetName();
}
