package cn.mc.agent.service;

import cn.mc.agent.entity.EpisodicEvent;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

public interface EpisodicMemoryService extends IService<EpisodicEvent> {

    /**
     * 从 ai_session 的 question+answer 中提取结构化事件，写入 episodic_memory
     * @param aiSessionId ai_session 表的主键 ID
     */
    void generateEvents(Long aiSessionId);

    /**
     * 查询某会话的所有事件
     */
    List<EpisodicEvent> findBySessionId(String sessionId);

    /**
     * 按主题关键词搜索事件
     */
    List<EpisodicEvent> searchByTopic(String sessionId, String topicKeyword);

    /**
     * 获取最近 N 条事件
     */
    List<EpisodicEvent> getRecentEvents(String sessionId, int limit);
}
