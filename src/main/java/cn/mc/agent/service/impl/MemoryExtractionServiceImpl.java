package cn.mc.agent.service.impl;

import cn.mc.agent.entity.AiSession;
import cn.mc.agent.entity.EpisodicEvent;
import cn.mc.agent.prompts.PlanExecutePrompts;
import cn.mc.agent.service.*;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class MemoryExtractionServiceImpl implements MemoryExtractionService {

    private static final int MIN_ANSWER_LENGTH = 2000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ChatModel chatModel;
    private final AiSessionService aiSessionService;
    private final EpisodicMemoryService episodicMemoryService;
    private final SemanticMemoryService semanticMemoryService;
    private final ProceduralMemoryService proceduralMemoryService;

    public MemoryExtractionServiceImpl(ChatModel chatModel,
                                       AiSessionService aiSessionService,
                                       EpisodicMemoryService episodicMemoryService,
                                       SemanticMemoryService semanticMemoryService,
                                       ProceduralMemoryService proceduralMemoryService) {
        this.chatModel = chatModel;
        this.aiSessionService = aiSessionService;
        this.episodicMemoryService = episodicMemoryService;
        this.semanticMemoryService = semanticMemoryService;
        this.proceduralMemoryService = proceduralMemoryService;
    }

    @Override
    public void extractAll(Long aiSessionId) {
        AiSession session = aiSessionService.getById(aiSessionId);
        if (session == null || session.getAnswer() == null) {
            return;
        }
        if (session.getAnswer().length() < MIN_ANSWER_LENGTH) {
            return;
        }

        // 检查是否已有事件记忆（避免重复提取）
        List<EpisodicEvent> existingEvents = episodicMemoryService.findBySessionId(session.getSessionId());
        if (!existingEvents.isEmpty()) {
            log.debug("事件记忆已存在，跳过提取: sessionId={}", session.getSessionId());
            return;
        }

        try {
            String userPrompt = "## 用户问题\n" + session.getQuestion() + "\n\n## 研究报告\n" + session.getAnswer();
            String response = ChatClient.builder(chatModel).build()
                    .prompt()
                    .system(PlanExecutePrompts.MEMORY_EXTRACTION)
                    .user(userPrompt)
                    .call()
                    .content();

            if (response == null || response.isBlank()) {
                return;
            }

            String jsonStr = extractJson(response);
            JSONObject result = JSON.parseObject(jsonStr);

            // 1. 写入事件记忆
            saveEpisodicEvents(session, result.getJSONArray("episodic"));

            // 2. 写入语义记忆
            String userId = session.getSessionId(); // 暂用 sessionId 作为 userId
            List<JSONObject> semanticUpdates = result.getJSONArray("semantic") != null
                    ? result.getJSONArray("semantic").toJavaList(JSONObject.class) : List.of();
            semanticMemoryService.updateMemory(userId, semanticUpdates);

            // 3. 写入程序记忆
            List<JSONObject> proceduralUpdates = result.getJSONArray("procedural") != null
                    ? result.getJSONArray("procedural").toJavaList(JSONObject.class) : List.of();
            proceduralMemoryService.updateMemory(userId, proceduralUpdates);

            log.info("统一记忆提取完成: sessionId={}, 事件数={}, 语义更新数={}, 程序更新数={}",
                    session.getSessionId(),
                    result.getJSONArray("episodic") != null ? result.getJSONArray("episodic").size() : 0,
                    semanticUpdates.size(),
                    proceduralUpdates.size());
        } catch (Exception e) {
            log.error("统一记忆提取失败: aiSessionId={}", aiSessionId, e);
        }
    }

    private void saveEpisodicEvents(AiSession session, com.alibaba.fastjson2.JSONArray episodicArray) {
        if (episodicArray == null) return;

        for (int i = 0; i < episodicArray.size(); i++) {
            JSONObject event = episodicArray.getJSONObject(i);
            EpisodicEvent ep = new EpisodicEvent();
            ep.setSessionId(session.getSessionId());
            ep.setConversationId(session.getSessionId());
            ep.setEventType(event.getString("type"));
            ep.setContent(event.getString("content"));
            ep.setTopic(event.getString("topic"));
            ep.setCreateTime(LocalDateTime.now());
            episodicMemoryService.save(ep);
        }
    }

    private String extractJson(String response) {
        String trimmed = response.trim();
        if (trimmed.startsWith("{")) {
            return trimmed;
        }
        int start = trimmed.indexOf("{");
        int end = trimmed.lastIndexOf("}");
        if (start >= 0 && end > start) {
            return trimmed.substring(start, end + 1);
        }
        return trimmed;
    }
}
