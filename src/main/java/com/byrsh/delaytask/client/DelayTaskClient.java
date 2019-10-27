package com.byrsh.delaytask.client;

import com.byrsh.delaytask.Config;
import com.byrsh.delaytask.Task;
import com.byrsh.delaytask.util.JsonMapper;
import com.byrsh.delaytask.util.ScriptUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: yangrusheng
 * @Description:
 * @Date: Created in 10:08 2019/8/13
 * @Modified By:
 */
public class DelayTaskClient {

    private static final Logger LOGGER = LogManager.getLogger(DelayTaskClient.class);

    /**
     * jedisPool
     */
    private final JedisPool jedisPool;

    /**
     * 配置
     */
    private final Config config;

    private static final String POP_LUA = "/script/pop.lua";
    private static final String POP_SPECIFY_LUA = "/script/pop_specify.lua";

    private String popLuaSha;
    private String popSpecifyLuaSha;


    public DelayTaskClient(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
        this.config = new Config();
        //加载lua脚本
        loadRedisScripts();
    }

    public DelayTaskClient(Config config) {
        if (config == null) {
            this.config = new Config();
        } else {
            this.config = config;
        }
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        if (this.config.getRedisMaxTotal() > 0) {
            poolConfig.setMaxTotal(this.config.getRedisMaxTotal());
        }
        if (this.config.getRedisMaxIdle() > 0) {
            poolConfig.setMaxIdle(this.config.getRedisMaxIdle());
        }
        if (this.config.getRedisMinIdle() > 0) {
            poolConfig.setMinIdle(this.config.getRedisMinIdle());
        }
        poolConfig.setMaxWaitMillis(this.config.getRedisMaxWaitMillis());
        this.jedisPool = new JedisPool(poolConfig, this.config.getRedisHost(), this.config.getRedisPort(),
                this.config.getRedisTimeout(), this.config.getRedisPassword(), this.config.getRedisIndex());
        //加载lua脚本
        loadRedisScripts();
    }

    /**
     * 添加任务
     * @param task 延迟任务
     * @param key  zset key
     * @return 执行结果
     */
    public Boolean addTask(Task task, String key) {
        if (task == null || key == null || key.length() == 0) {
            throw new IllegalArgumentException("task must not be null, key must not be null or empty string.");
        }
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.zadd(key, task.getDelayTime().doubleValue(), JsonMapper.writeValueAsString(task));
            return true;
        } catch (Exception e) {
            LOGGER.error("delay task client add task exception: ", e);
            return false;
        }
    }

    /**
     * 添加任务
     * @param tasks 延迟任务列表
     * @param key  zset key
     * @return 执行结果
     */
    public Boolean addTasks(List<Task> tasks, String key) {
        if (tasks == null || tasks.isEmpty() || key == null || key.length() == 0) {
            throw new IllegalArgumentException("tasks must not be null or empty, key must not be null or "
                    + "empty String.");
        }
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, Double> scoreMembers = new HashMap<>();
            for (Task task : tasks) {
                scoreMembers.put(JsonMapper.writeValueAsString(task), task.getDelayTime().doubleValue());
            }
            jedis.zadd(key, scoreMembers);
            return true;
        } catch (Exception e) {
            LOGGER.error("delay task client add task exception: ", e);
            return false;
        }
    }

    /**
     * 获取任务
     * @param key 存储任务有序集合 key
     * @return 延迟任务
     */
    public Task popTask(String key) {
        if (key == null || key.length() == 0) {
            throw new IllegalArgumentException("key must not be null or empty String.");
        }
        List<Task> tasks = popTasks(key, 1);
        if (tasks == null || tasks.isEmpty()) {
            return null;
        } else {
            return tasks.get(0);
        }
    }

    /**
     * 获取指定数量的到时任务
     * @param key 存储任务有序集合 key
     * @param number  数量
     * @return 延迟任务列表
     */
    public List<Task> popTasks(String key, int number) {
        return moveTasks(key, key + "_executing", number, System.currentTimeMillis());
    }

    /**
     * 取消未执行的任务
     * @param task  任务
     * @param key  zset key
     * @return 执行结果
     */
    public Boolean cancelTask(Task task, String key) {
        if (task == null || key == null || key.length() == 0) {
            throw new IllegalArgumentException("task must not be null, key must not be null or empty string.");
        }
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.zrem(key, JsonMapper.writeValueAsString(task));
            return true;
        } catch (Exception e) {
            LOGGER.error("delay task client add task exception: ", e);
            return false;
        }
    }

    /**
     * 移除已经执行过的任务
     * @param task  task
     * @param key  zset key
     * @return 执行结果
     */
    public Boolean removeExecutedTask(Task task, String key) {
        return cancelTask(task, key + "_executing");
    }

    /**
     * 移除过期任务
     * @param task  task
     * @param key  zset key
     * @return 执行结果
     */
    public Boolean removeExpireTask(Task task, String key) {
        return removeExecutedTask(task, key);
    }

    /**
     * 重新添加客户端超时处理的任务，用于定时检查线程
     * @param key  zset key
     * @return
     */
    public Boolean reAddTimeoutHandlerTask(String key, int number, long executeTimeoutTime) {
        List<Task> tasks = moveTasks(key + "_executing", key, number, System.currentTimeMillis() - executeTimeoutTime);
        if (tasks != null) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 客户端线程池任务队列已满时，则重新添加任务至延迟任务集合，会把该任务从执行中任务集合中移除。
     * @param task  task
     * @param key  zset key
     * @return  执行结果
     */
    public Boolean reAddTask(Task task, String key) {
        if (task == null || key == null || key.length() == 0) {
            throw new IllegalArgumentException("task must not be null, key must not be null or empty string.");
        }
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.evalsha(popSpecifyLuaSha, 2, key + "_executing", key,
                    JsonMapper.writeValueAsString(task), String.valueOf(task.getDelayTime()));
            return true;
        } catch (Exception e) {
            LOGGER.error("delay task client re add task exception: ", e);
            return false;
        }
    }

    private void loadRedisScripts() {
        try (Jedis jedis = jedisPool.getResource()) {
            this.popLuaSha = jedis.scriptLoad(ScriptUtil.readScript(POP_LUA));
            this.popSpecifyLuaSha = jedis.scriptLoad(ScriptUtil.readScript(POP_SPECIFY_LUA));
        } catch (Exception e) {
            LOGGER.error("delay task client load redis scripts exception: ", e);
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Task> moveTasks(String sourceKey, String targetKey, int number, long score) {
        if (sourceKey == null || sourceKey.length() == 0 || targetKey == null || targetKey.length() == 0) {
            throw new IllegalArgumentException("key must not be null or empty String.");
        }
        if (number <= 0) {
            return new ArrayList<>();
        }
        try (Jedis jedis = jedisPool.getResource()) {
            List<String> list = (List<String>) jedis.evalsha(popLuaSha, 2, sourceKey, targetKey,
                    String.valueOf(number), String.valueOf(score));
            StringBuilder sb = new StringBuilder();
            String json = "[";
            for (String item: list) {
                sb.append(item);
                sb.append(",");
            }
            if (sb.length() > 1) {
                json += sb.substring(0, sb.length() - 1);
            }
            json += "]";
            return JsonMapper.readValue(json, new TypeReference<List<Task>>() {});
        } catch (Exception e) {
            LOGGER.error("delay task client move tasks exception: ", e);
            return new ArrayList<>();
        }
    }

}
