package com.byrsh.delaytask.client;

import com.byrsh.delaytask.Config;
import com.byrsh.delaytask.Task;
import com.byrsh.delaytask.util.JsonMapper;
import com.byrsh.delaytask.util.ScriptUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisNoScriptException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    private final JedisCluster jedisCluster;

    private final JedisSentinelPool jedisSentinelPool;

    /**
     * 配置
     */
    private final Config config;

    private static Map<String, Boolean> scriptLoadMap = new ConcurrentHashMap<>(8);

    private static final String POP_LUA = "/script/pop.lua";
    private static final String POP_SPECIFY_LUA = "/script/pop_specify.lua";

    private String popLuaSha;
    private String popSpecifyLuaSha;


    public DelayTaskClient(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
        this.jedisCluster = null;
        this.jedisSentinelPool = null;
        this.config = new Config();
    }

    public DelayTaskClient(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
        this.jedisPool = null;
        this.jedisSentinelPool = null;
        this.config = new Config();
    }

    public DelayTaskClient(JedisSentinelPool jedisSentinelPool) {
        this.jedisSentinelPool = jedisSentinelPool;
        this.jedisCluster = null;
        this.jedisPool = null;
        this.config = new Config();
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
        if (this.config.getClusterNodes() != null) {
            String[] clusterNodes = this.config.getClusterNodes().split(",");
            Set<HostAndPort> nodes = new HashSet<>();
            for (String nodeStr: clusterNodes) {
                String[] nodeInfo = nodeStr.split(":");
                nodes.add(new HostAndPort(nodeInfo[0], Integer.valueOf(nodeInfo[1])));
            }
            this.jedisCluster = new JedisCluster(nodes, this.config.getRedisTimeout(), poolConfig);
            this.jedisPool = null;
            this.jedisSentinelPool = null;
        } else if (this.config.getSentinelNodes() != null) {
            if (this.config.getSentinelMaster() == null || "".equals(this.config.getSentinelMaster())) {
                throw new IllegalArgumentException("redis sentinel master must not be null.");
            }
            String[] sentinelNodes = this.config.getSentinelNodes().split(",");
            Set<String> nodes = new HashSet<>();
            for (String nodeStr: sentinelNodes) {
                nodes.add(nodeStr);
            }
            this.jedisSentinelPool = new JedisSentinelPool(this.config.getSentinelMaster(), nodes, poolConfig);
            this.jedisCluster = null;
            this.jedisPool = null;
        }  else {
            this.jedisPool = new JedisPool(poolConfig, this.config.getRedisHost(), this.config.getRedisPort(),
                    this.config.getRedisTimeout(), this.config.getRedisPassword(), this.config.getRedisIndex());
            this.jedisCluster = null;
            this.jedisSentinelPool = null;
        }
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
        key = hashTagKey(key);
        if (jedisCluster != null) {
            try {
                jedisCluster.zadd(key, task.getDelayTime().doubleValue(), JsonMapper.writeValueAsString(task));
                return true;
            } catch (Exception e) {
                LOGGER.error("delay task client add task exception: ", e);
                return false;
            }
        } else {
            Jedis jedis = null;
            try {
                if (jedisSentinelPool != null) {
                    jedis = jedisSentinelPool.getResource();
                } else {
                    jedis = jedisPool.getResource();
                }
                jedis.zadd(key, task.getDelayTime().doubleValue(), JsonMapper.writeValueAsString(task));
                return true;
            } catch (Exception e) {
                LOGGER.error("delay task client add task exception: ", e);
                return false;
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
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
        key = hashTagKey(key);
        Map<String, Double> scoreMembers = new HashMap<>();
        for (Task task : tasks) {
            scoreMembers.put(JsonMapper.writeValueAsString(task), task.getDelayTime().doubleValue());
        }
        if (jedisCluster != null) {
            try {
                jedisCluster.zadd(key, scoreMembers);
                return true;
            } catch (Exception e) {
                LOGGER.error("delay task client add task exception: ", e);
                return false;
            }
        } else {
            Jedis jedis = null;
            try {
                if (jedisSentinelPool != null) {
                    jedis = jedisSentinelPool.getResource();
                } else {
                    jedis = jedisPool.getResource();
                }
                jedis.zadd(key, scoreMembers);
                return true;
            } catch (Exception e) {
                LOGGER.error("delay task client add task exception: ", e);
                return false;
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
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
        key = hashTagKey(key);
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
        key = hashTagKey(key);
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
        key = hashTagKey(key);
        if (jedisCluster != null) {
            try {
                jedisCluster.zrem(key, JsonMapper.writeValueAsString(task));
                return true;
            } catch (Exception e) {
                LOGGER.error("delay task client add task exception: ", e);
                return false;
            }
        } else {
            Jedis jedis = null;
            try {
                if (jedisSentinelPool != null) {
                    jedis = jedisSentinelPool.getResource();
                } else {
                    jedis = jedisPool.getResource();
                }
                jedis.zrem(key, JsonMapper.writeValueAsString(task));
                return true;
            } catch (Exception e) {
                LOGGER.error("delay task client add task exception: ", e);
                return false;
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
    }

    /**
     * 移除已经执行过的任务
     * @param task  task
     * @param key  zset key
     * @return 执行结果
     */
    public Boolean removeExecutedTask(Task task, String key) {
        key = hashTagKey(key);
        return cancelTask(task, key + "_executing");
    }

    /**
     * 移除过期任务
     * @param task  task
     * @param key  zset key
     * @return 执行结果
     */
    public Boolean removeExpireTask(Task task, String key) {
        key = hashTagKey(key);
        return removeExecutedTask(task, key);
    }

    /**
     * 重新添加客户端超时处理的任务，用于定时检查线程
     * @param key  zset key
     * @return
     */
    public Boolean reAddTimeoutHandlerTask(String key, int number, long executeTimeoutTime) {
        key = hashTagKey(key);
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
        key = hashTagKey(key);
        if (scriptLoadMap.get(key) == null || !scriptLoadMap.get(key)) {
            loadRedisScripts(key);
        }
        if (jedisCluster != null) {
            try {
                jedisCluster.evalsha(popSpecifyLuaSha, 2, key + "_executing", key,
                        JsonMapper.writeValueAsString(task), String.valueOf(task.getDelayTime()));
                return true;
            } catch (JedisNoScriptException e) {
                loadRedisScripts(key);
                LOGGER.error("delay task client re add task exception, reload scripts. Exception: ", e);
                return false;
            } catch (Exception e) {
                LOGGER.error("delay task client re add task exception: ", e);
                return false;
            }
        } else {
            Jedis jedis = null;
            try {
                if (jedisSentinelPool != null) {
                    jedis = jedisSentinelPool.getResource();
                } else {
                    jedis = jedisPool.getResource();
                }
                jedis.evalsha(popSpecifyLuaSha, 2, key + "_executing", key,
                        JsonMapper.writeValueAsString(task), String.valueOf(task.getDelayTime()));
                return true;
            } catch (JedisNoScriptException e) {
                loadRedisScripts(key);
                LOGGER.error("delay task client re add task exception, reload scripts. Exception: ", e);
                return false;
            } catch (Exception e) {
                LOGGER.error("delay task client re add task exception: ", e);
                return false;
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
    }

    /**
     * 加载脚本
     * @param key slotKey
     */
    private void loadRedisScripts(String key) {
        key = hashTagKey(key);
        if (jedisCluster != null) {
            try {
                this.popLuaSha = jedisCluster.scriptLoad(ScriptUtil.readScript(POP_LUA), key);
                this.popSpecifyLuaSha = jedisCluster.scriptLoad(ScriptUtil.readScript(POP_SPECIFY_LUA), key);
                scriptLoadMap.put(key, true);
            } catch (Exception e) {
                LOGGER.error("delay task client load redis scripts exception: ", e);
                throw new RuntimeException(e);
            }
        } else {
            Jedis jedis = null;
            try {
                if (jedisSentinelPool != null) {
                    jedis = jedisSentinelPool.getResource();
                } else {
                    jedis = jedisPool.getResource();
                }
                this.popLuaSha = jedis.scriptLoad(ScriptUtil.readScript(POP_LUA));
                this.popSpecifyLuaSha = jedis.scriptLoad(ScriptUtil.readScript(POP_SPECIFY_LUA));
                scriptLoadMap.put(key, true);
            } catch (Exception e) {
                LOGGER.error("delay task client load redis scripts exception: ", e);
                throw new RuntimeException(e);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
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
        if (scriptLoadMap.get(sourceKey) == null || !scriptLoadMap.get(sourceKey)) {
            loadRedisScripts(sourceKey);
        }
        if (jedisCluster != null) {
            try {
                List<String> list = (List<String>) jedisCluster.evalsha(popLuaSha, 2, sourceKey, targetKey,
                        String.valueOf(number), String.valueOf(score));
                return parseTasks(list);
            } catch (JedisNoScriptException e) {
                loadRedisScripts(sourceKey);
                LOGGER.error("delay task client move tasks exception, reload scripts. Exception: ", e);
                return new ArrayList<>();
            } catch (Exception e) {
                LOGGER.error("delay task client move tasks exception: ", e);
                return new ArrayList<>();
            }

        } else {
            Jedis jedis = null;
            try {
                if (jedisSentinelPool != null) {
                    jedis = jedisSentinelPool.getResource();
                } else {
                    jedis = jedisPool.getResource();
                }
                List<String> list = (List<String>) jedis.evalsha(popLuaSha, 2, sourceKey, targetKey,
                        String.valueOf(number), String.valueOf(score));
                return parseTasks(list);
            } catch (JedisNoScriptException e) {
                loadRedisScripts(sourceKey);
                LOGGER.error("delay task client move tasks exception, reload scripts. Exception: ", e);
                return new ArrayList<>();
            } catch (Exception e) {
                LOGGER.error("delay task client move tasks exception: ", e);
                return new ArrayList<>();
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
    }

    private List<Task> parseTasks(List<String> list) {
        StringBuilder sb = new StringBuilder();
        String json = "[";
        for (String item : list) {
            sb.append(item);
            sb.append(",");
        }
        if (sb.length() > 1) {
            json += sb.substring(0, sb.length() - 1);
        }
        json += "]";
        return JsonMapper.readValue(json, new TypeReference<List<Task>>() {
        });
    }

    /**
     * 使用 key hash tag
     * @return
     */
    private String hashTagKey(String key) {
        if (key == null) {
            return key;
        }
        if (key.startsWith("{") && key.contains("}")) {
            return key;
        } else {
            return "{" + key + "}";
        }
    }

}
