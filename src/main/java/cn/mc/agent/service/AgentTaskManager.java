package cn.mc.agent.service;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author tmengchun
 * @description
 * @create 2026/4/24 12:46
 */
@Component
@Slf4j
public class AgentTaskManager implements InitializingBean, DisposableBean {

    private static final String TASK_KEY_PREFIX = "agent:task:";
    private static final long TASK_TTL_MINUTES = 30;
    private static final String STOP_TOPIC_NAME = "agent:stop";

    /**
     * 当前实例的唯一标识
     */
    private final String instanceId;
    /**
     * Redisson 客户端
     */
    private final RedissonClient redissonClient;

    /**
     * 停止消息的发布订阅主题
     */
    private final RTopic stopTopic;

    private final Map<String, TaskInfo> taskMap = new ConcurrentHashMap<>();



    public AgentTaskManager(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
        this.instanceId = UUID.randomUUID().toString().substring(0, 8);
        this.stopTopic = redissonClient.getTopic(STOP_TOPIC_NAME, StringCodec.INSTANCE);
        log.info("AgentTaskManager 初始化, instanceId={}", instanceId);
    }


    public TaskInfo registerTask(String conversationId, Sinks.Many<String> sink, String agentType) {
        TaskInfo existingTask = taskMap.get(conversationId);
        if (existingTask != null) {
            //说明当前conversationId已经存在一个正在活跃的任务
            return null;
        }

        // 在redis中注册任务
        RBucket<String> bucket = getTaskBucket(conversationId);
        boolean acquired = bucket.trySet(instanceId, TASK_TTL_MINUTES, TimeUnit.MINUTES);

        if (!acquired) {
            String holder = bucket.get();
            log.warn("会话 {} 已在实例 {} 上执行，当前实例 {} 拒绝注册", conversationId, holder, instanceId);
            return null;
        }

        // 3. 注册到本地
        TaskInfo taskInfo = new TaskInfo(sink, agentType);
        taskMap.put(conversationId, taskInfo);
        log.info("注册任务成功: conversationId={}, agentType={}, instanceId={}", conversationId, agentType, instanceId);
        return taskInfo;
    }

    public boolean stopTask(String conversationId) {
        // TODO:为什么先删除本机里的任务
        // 快速路径：先检查本地是否存在任务，如果存在，直接停止并删除（避免不必要的Redis访问和广播）
        TaskInfo localTask = taskMap.get(conversationId);
        if (localTask != null) {
            log.info("本机停止任务: conversationId={}, instanceId={}", conversationId, instanceId);
            doStopTask(conversationId,localTask);
            return true;
        }

        // 检查Redis中是否存在任务，如果存在，说明其他实例上有任务在执行，需要删除Redis中的标记并进行广播
        RBucket<String> bucket = getTaskBucket(conversationId);
        if (!bucket.isExists()) {
            log.info("没有找到任务: conversationId={}, instanceId={}", conversationId, instanceId);
            return false;
        }

        // 找到任务，判断是否是当前实例持有
        String holder = bucket.get();
        if (instanceId.equals(holder)) {
            log.debug("当前实例持有任务，准备停止: conversationId={}, instanceId={}", conversationId, instanceId);
            return false; // 当本机任务被清理但是Redis标记未及时删除时，可能会出现这种情况，说明已经有请求来停止任务了
        }
        // 其他实例持有任务，删除Redis标记并广播停止
        long receivers = stopTopic.publish(conversationId);
        log.info("发布停止任务广播: conversationId={}, instanceId={}, receivers={}", conversationId, instanceId, receivers);
        return true;
    }

    public void doStopTask(String conversationId, TaskInfo taskInfo) {
        try {
            // 1. 中断底层调用
            Disposable disposable = taskInfo.getDisposable();
            if (disposable != null && !disposable.isDisposed()) {
                disposable.dispose();
                log.info("已中断底层调用: conversationId={}", conversationId);
            }

            // 2. 发送停止消息
            Sinks.Many<String> sink = taskInfo.getSink();
            if (sink != null) {
                try {
                    sink.tryEmitNext(createStopMessage());
                    sink.tryEmitComplete();
                    log.info("已发送停止消息: conversationId={}", conversationId);
                } catch (Exception e) {
                    log.warn("发送停止消息失败: conversationId={}", conversationId, e);
                }
            }
        } finally {
            // 3. 清理本地和 Redis
            doRemoveTask(conversationId);
        }
    }

    /**
     * 内部移除：从本地 map 删除 + 删除 Redis key
     */
    private void doRemoveTask(String conversationId) {
        taskMap.remove(conversationId);

        RBucket<String> bucket = getTaskBucket(conversationId);
        String holder = bucket.get();
        if (instanceId.equals(holder)) {
            bucket.delete();
            log.debug("删除 Redis 任务key: conversationId={}", conversationId);
        }
    }


    public void setDisposable(String conversationId, Disposable disposable) {
        TaskInfo taskInfo = taskMap.get(conversationId);
        if (taskInfo != null) {
            taskInfo.setDisposable(disposable);
        }
    }

    /**
     * 创建停止消息
     */
    private String createStopMessage() {
        JSONObject obj = new JSONObject();
        obj.put("type", "text");
        obj.put("content", "⏹ 用户已停止生成\n");
        return JSON.toJSONString(obj);
    }


    /**
     * 获取任务 RBucket
     */
    private RBucket<String> getTaskBucket(String conversationId) {
        return redissonClient.getBucket(TASK_KEY_PREFIX + conversationId, StringCodec.INSTANCE);
    }

    public boolean hasRunningTask(String conversationId) {
        // 先检查本地（快速路径）
        if (taskMap.containsKey(conversationId)) {
            return true;
        }
        // 检查 Redis（其他实例可能持有）
        RBucket<String> bucket = getTaskBucket(conversationId);
        return bucket.isExists();
    }




    @Override
    public void destroy() throws Exception {

    }

    @Override
    public void afterPropertiesSet() throws Exception {

    }

    public static class TaskInfo {
        private final Sinks.Many<String> sink;
        private Disposable disposable;
        private final long createTime;
        private String agentType;

        public TaskInfo(Sinks.Many<String> sink, String agentType) {
            this.sink = sink;
            this.agentType = agentType;
            this.createTime = System.currentTimeMillis();
        }

        public Sinks.Many<String> getSink() {
            return sink;
        }

        public Disposable getDisposable() {
            return disposable;
        }

        public void setDisposable(Disposable disposable) {
            this.disposable = disposable;
        }

        public long getCreateTime() {
            return createTime;
        }

        public String getAgentType() {
            return agentType;
        }
    }
}
