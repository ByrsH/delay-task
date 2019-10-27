package com.byrsh.delaytask;


import java.io.Serializable;

/**
 * @Author: yangrusheng
 * @Description:
 * @Date: Created in 10:47 2019/8/13
 * @Modified By:
 */
public class Task implements Serializable {
    /**
     * 任务内容
     */
    private String content;

    /**
     * 延迟任务执行时间
     * 值为时间戳
     */
    private Long delayTime;

    /**
     * 创建时间
     */
    private Long createTime;

    /**
     * 过期时间，超过这个时间将不再执行该任务
     * 0 或 null 表示永不超时。除非任务执行成功主动删除，否则将一直保留在延迟任务集合中。
     * expireTime < now time 则任务将不执行，并且被从集合中删除
     */
    private Long expireTime;

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Long getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(Long delayTime) {
        this.delayTime = delayTime;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public Long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(Long expireTime) {
        this.expireTime = expireTime;
    }
}
