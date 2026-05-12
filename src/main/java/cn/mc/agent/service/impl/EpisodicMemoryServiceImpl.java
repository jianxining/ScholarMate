package cn.mc.agent.service.impl;

import cn.mc.agent.entity.AiSession;
import cn.mc.agent.entity.EpisodicEvent;
import cn.mc.agent.mapper.EpisodicEventMapper;
import cn.mc.agent.prompts.PlanExecutePrompts;
import cn.mc.agent.service.AiSessionService;
import cn.mc.agent.service.EpisodicMemoryService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class EpisodicMemoryServiceImpl extends ServiceImpl<EpisodicEventMapper, EpisodicEvent> implements EpisodicMemoryService {

    private static final int MIN_ANSWER_LENGTH = 2000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ChatModel chatModel;
    private final AiSessionService aiSessionService;

    public EpisodicMemoryServiceImpl(ChatModel chatModel, AiSessionService aiSessionService) {
        this.chatModel = chatModel;
        this.aiSessionService = aiSessionService;
    }

    @Override
    public void generateEvents(Long aiSessionId) {
        AiSession session = aiSessionService.getById(aiSessionId);
        if (session == null || session.getAnswer() == null) {
            return;
        }
        if (session.getAnswer().length() < MIN_ANSWER_LENGTH) {
            return;
        }
        // 检查是否已有事件
        List<EpisodicEvent> existing = findBySessionId(session.getSessionId());
        if (!existing.isEmpty()) {
            return;
        }

        try {
            String userPrompt = "## 用户问题\n" + session.getQuestion() + "\n\n## 研究报告\n" + session.getAnswer();
            String response = ChatClient.builder(chatModel).build()
                    .prompt()
                    .system(PlanExecutePrompts.EPISODIC_EXTRACTION)
                    .user(userPrompt)
                    .call()
                    .content();

            if (response == null || response.isBlank()) {
                return;
            }

            // 解析 JSON 数组
            String jsonStr = extractJsonArray(response);
            List<Map<String, String>> events = MAPPER.readValue(jsonStr, new TypeReference<>() {});

            for (Map<String, String> event : events) {
                EpisodicEvent ep = new EpisodicEvent();
                ep.setSessionId(session.getSessionId());
                ep.setConversationId(session.getSessionId());
                ep.setEventType(event.get("type"));
                ep.setContent(event.get("content"));
                ep.setTopic(event.get("topic"));
                ep.setCreateTime(LocalDateTime.now());
                this.save(ep);
            }
            log.info("事件记忆生成成功: sessionId={}, 事件数={}", session.getSessionId(), events.size());
        } catch (Exception e) {
            log.error("事件记忆生成失败: aiSessionId={}", aiSessionId, e);
        }
    }

    @Override
    public List<EpisodicEvent> findBySessionId(String sessionId) {
        return baseMapper.findBySessionId(sessionId);
    }

    @Override
    public List<EpisodicEvent> searchByTopic(String sessionId, String topicKeyword) {
        return baseMapper.findBySessionIdAndTopic(sessionId, topicKeyword);
    }

    @Override
    public List<EpisodicEvent> getRecentEvents(String sessionId, int limit) {
        return baseMapper.findRecentBySessionId(sessionId, limit);
    }

    /**
     * 从 LLM 响应中提取 JSON 数组（兼容被包裹在 markdown 代码块中的情况）
     */
    private String extractJsonArray(String response) {
        String trimmed = response.trim();
        if (trimmed.startsWith("[")) {
            return trimmed;
        }
        // 处理 ```json ... ``` 包裹的情况
        int start = trimmed.indexOf("[");
        int end = trimmed.lastIndexOf("]");
        if (start >= 0 && end > start) {
            return trimmed.substring(start, end + 1);
        }
        return trimmed;
    }
}
