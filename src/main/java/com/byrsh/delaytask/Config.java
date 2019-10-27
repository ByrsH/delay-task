package com.byrsh.delaytask;

/**
 * @Author: yangrusheng
 * @Description: 配置信息类
 * @Date: Created in 9:14 2019/8/13
 * @Modified By:
 */
public class Config {

    /**
     * redis host
     */
    private String redisHost;

    /**
     * redis port
     */
    private int redisPort;

    /**
     * redis password
     */
    private String redisPassword = null;

    /**
     * 连接指定下标 redis 数据库
     */
    private int redisIndex = 0;

    /**
     * 连接超时时间，单位：ms
     */
    private int redisTimeout;

    /**
     * redisPool最大连接数
     */
    private int redisMaxTotal;

    /**
     * redisPool最大空闲数
     */
    private int redisMaxIdle;

    /**
     * redisPool最小空闲数
     */
    private int redisMinIdle;

    /**
     * 获取 redis 连接时的最大等待毫秒数
     */
    private long redisMaxWaitMillis = 20000L;

    /**
     * 延迟任务处理线程池核心线程数
     */
    private int corePoolSize;

    /**
     * 延迟任务处理线程池最大线程数
     */
    private int maxPoolSize;

    /**
     * 线程池空闲线程存活时间, 单位：s
     */
    private long keepAliveTime;

    /**
     * 线程池最大处理任务数
     */
    private int maxTaskAmount;

    /**
     * worker 线程从redis获取task的间隔时间
     */
    private int workerSleepTime;

    /**
     * 每次从redis获取任务数
     */
    private int taskNumber;

    /**
     * 延时任务处理类包名
     */
    private String scanPackageName;

    /**
     * 检查线程间隔执行时间
     */
    private long checkoutSleepTime;

    /**
     * 任务执行超时时间，超过将重新放入待执行集合
     */
    private long executeTimeoutTime;

    public long getCheckoutSleepTime() {
        return checkoutSleepTime;
    }

    public void setCheckoutSleepTime(long checkoutSleepTime) {
        this.checkoutSleepTime = checkoutSleepTime;
    }

    public long getExecuteTimeoutTime() {
        return executeTimeoutTime;
    }

    public void setExecuteTimeoutTime(long executeTimeoutTime) {
        this.executeTimeoutTime = executeTimeoutTime;
    }

    public String getScanPackageName() {
        return scanPackageName;
    }

    public void setScanPackageName(String scanPackageName) {
        this.scanPackageName = scanPackageName;
    }

    public int getTaskNumber() {
        return taskNumber;
    }

    public void setTaskNumber(int taskNumber) {
        this.taskNumber = taskNumber;
    }

    public int getWorkerSleepTime() {
        return workerSleepTime;
    }

    public void setWorkerSleepTime(int workerSleepTime) {
        this.workerSleepTime = workerSleepTime;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public String getRedisPassword() {
        return redisPassword;
    }

    public void setRedisPassword(String redisPassword) {
        this.redisPassword = redisPassword;
    }

    public int getRedisIndex() {
        return redisIndex;
    }

    public void setRedisIndex(int redisIndex) {
        this.redisIndex = redisIndex;
    }

    public int getRedisTimeout() {
        return redisTimeout;
    }

    public void setRedisTimeout(int redisTimeout) {
        this.redisTimeout = redisTimeout;
    }

    public int getRedisMaxTotal() {
        return redisMaxTotal;
    }

    public void setRedisMaxTotal(int redisMaxTotal) {
        this.redisMaxTotal = redisMaxTotal;
    }

    public int getRedisMaxIdle() {
        return redisMaxIdle;
    }

    public void setRedisMaxIdle(int redisMaxIdle) {
        this.redisMaxIdle = redisMaxIdle;
    }

    public int getRedisMinIdle() {
        return redisMinIdle;
    }

    public void setRedisMinIdle(int redisMinIdle) {
        this.redisMinIdle = redisMinIdle;
    }

    public long getRedisMaxWaitMillis() {
        return redisMaxWaitMillis;
    }

    public void setRedisMaxWaitMillis(long redisMaxWaitMillis) {
        this.redisMaxWaitMillis = redisMaxWaitMillis;
    }

    public int getCorePoolSize() {
        return corePoolSize;
    }

    public void setCorePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public long getKeepAliveTime() {
        return keepAliveTime;
    }

    public void setKeepAliveTime(long keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }

    public int getMaxTaskAmount() {
        return maxTaskAmount;
    }

    public void setMaxTaskAmount(int maxTaskAmount) {
        this.maxTaskAmount = maxTaskAmount;
    }
}
