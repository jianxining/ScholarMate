package cn.mc.agent.agent;


import cn.mc.agent.common.AgentResponse;
import cn.mc.agent.entity.AiSession;
import cn.mc.agent.prompts.ReactAgentPrompts;
import cn.mc.agent.service.AgentTaskManager;
import cn.mc.agent.service.AiSessionService;
import cn.mc.agent.service.EpisodicMemoryService;
import cn.mc.agent.entity.EpisodicEvent;
import com.alibaba.fastjson2.JSON;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.memory.MessageWindowChatMemory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.converter.BeanOutputConverter;
import org.springframework.core.ParameterizedTypeReference;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author tmengchun
 * @description
 * @create 2026/4/24 11:23
 */

// Agent基类，定义Agent的基本属性和方法，作为所有具体Agent的父类
@Slf4j
@Getter
@Setter
public abstract class BaseAgent {
    protected ChatModel chatModel;
    protected String name;
    protected ChatMemory chatMemory;
    protected AiSessionService sessionService;
    protected AgentTaskManager taskManager;
    protected EpisodicMemoryService episodicMemoryService;

    // 是否启用推荐问题功能
    protected boolean enableRecommendations = true;
    // 目前推荐的词条
    protected String currentRecommendations;

    protected String currentQuestion;
    protected String currentConversationId;
    protected Long currentSessionId;

    // 记录Agent在执行过程中使用过的工具
    protected Set<String> usedTools;

    // 计时器相关属性
    protected long startTime;
    protected long firstResponseTime;

    protected String agentType;


    public BaseAgent(ChatModel chatModel, String name,String agentType) {
        this.chatModel = chatModel;
        this.name = name;
        this.agentType = agentType;
    }


    // 子类需要实现的抽象方法，定义Agent执行任务的逻辑
    public abstract Flux<String> execute(String input, String conversationId);

    /**
     * 初始化计时器
     */
    protected void initTimers() {
        startTime = System.currentTimeMillis();
        firstResponseTime = 0;
    }

    protected long getTotalResponseTime() {
        if (startTime == 0) {
            return 0;
        }
        return System.currentTimeMillis() - startTime;
    }

    /**
     * 清除记录的已使用工具，准备下一轮对话或任务执行
     */
    protected void clearUsedTools() {
        usedTools.clear();
    }

    protected String getUsedToolsString() {
        if (usedTools == null || usedTools.isEmpty()) {
            return "";
        }
        return String.join(",", usedTools);
    }

    protected void loadChatHistory(String conversationId, List<Message> messages, boolean skipSystem, boolean addLabel) {
        if (conversationId != null && chatMemory != null) {
            List<Message> history = chatMemory.get(conversationId);
            if (!history.isEmpty()) {
                if (addLabel) {
                    messages.add(new UserMessage("对话历史："));
                }
                for (Message msg : history) {
                    if (skipSystem && msg instanceof SystemMessage) {
                        continue;
                    }
                    messages.add(msg);
                }
            }
        }
    }

    protected String generateRecommendations(String conversationId, String currentQuestion, String currentAnswer) {
        if (!enableRecommendations) {
            return null;
        }

        try {
            List<Message> messages = new ArrayList<>();

            // 1. 添加系统提示词
            messages.add(new SystemMessage(ReactAgentPrompts.getRecommendPrompt()));

            // 2. 添加历史消息
            loadChatHistory(conversationId, messages, true, true);

            // 3. 添加当前会话的消息（最新的消息，放在最后）
            messages.add(new UserMessage("当前会话："));
            messages.add(new UserMessage(currentQuestion));
            if (currentAnswer != null) {
                messages.add(new AssistantMessage(currentAnswer));
            }

            // 4. 添加格式说明消息
            // 使用 BeanOutputConverter 进行结构化输出
            BeanOutputConverter<List<String>> converter = new BeanOutputConverter<>(new ParameterizedTypeReference<>() {
            });

            // 添加格式说明消息
            messages.add(new UserMessage("请根据上述对话生成3个推荐问题。输出格式为：\n" + converter.getFormat()));

            // 5. 调用模型生成推荐问题
            String response = ChatClient.builder(chatModel).build()
                    .prompt()
                    .messages(messages)
                    .call()
                    .content();

            // 6. 使用 converter 转换响应
            if (response != null && !response.isEmpty()) {
                List<String> recommendations = converter.convert(response);
                if (recommendations != null && !recommendations.isEmpty()) {
                    String jsonStr = JSON.toJSONString(recommendations);
                    log.info("生成推荐问题成功: {}", jsonStr);
                    return jsonStr;
                }
            }

            log.warn("生成推荐问题失败，响应格式无效: {}", response);
            return null;
        } catch (Exception e) {
            log.error("生成推荐问题异常", e);
            return null;
        }
    }

    protected Flux<String> checkRunningTask(String conversationId) {
        if (conversationId != null && taskManager != null && taskManager.hasRunningTask(conversationId)) {
            return Flux.error(new IllegalStateException("该会话正在执行中，请稍后再试"));
        }
        return null;
    }

    protected AgentTaskManager.TaskInfo registerTask(String conversationId, Sinks.Many<String> sink) {
        if (conversationId != null && taskManager != null) {
            AgentTaskManager.TaskInfo taskInfo = taskManager.registerTask(conversationId, sink, agentType);
            if (taskInfo == null) {
                log.warn("任务注册失败: conversationId={}", conversationId);
            }
            return taskInfo;
        }
        return null;
    }

    /**
     * 最近 N 轮保留完整回答，更早的轮次使用事件记忆
     */
    private static final int FULL_MEMORY_ROUNDS = 3;

    public ChatMemory createPersistentChatMemory(String sessionId, int maxMessages) {
        if (sessionService == null) {
            log.warn("sessionService is null, cannot load chat memory");
            return MessageWindowChatMemory.builder().maxMessages(maxMessages).build();
        }
        // 查询数据库中的对话历史
        List<AiSession> history = sessionService.findRecentBySessionId(sessionId, maxMessages);

        // 创建 ChatMemory
        ChatMemory chatMemory = MessageWindowChatMemory.builder().maxMessages(maxMessages).build();
        // 将历史记录添加到 ChatMemory（按时间顺序）
        if (history != null && !history.isEmpty()) {
            int episodicUsedCount = 0;
            // 反转历史记录顺序，确保按时间顺序添加
            for (int i = history.size() - 1; i >= 0; i--) {
                AiSession record = history.get(i);
                // 添加用户问题
                if (record.getQuestion() != null) {
                    chatMemory.add(sessionId, new UserMessage(record.getQuestion()));
                }
                // 添加AI回复：近期完整，远期用事件记忆
                if (record.getAnswer() != null) {
                    int distanceFromLatest = history.size() - 1 - i;
                    if (distanceFromLatest >= FULL_MEMORY_ROUNDS && episodicMemoryService != null) {
                        // 尝试从事件记忆加载
                        String episodicContext = loadEpisodicContext(record.getSessionId());
                        if (episodicContext != null) {
                            chatMemory.add(sessionId, new AssistantMessage(episodicContext));
                            episodicUsedCount++;
                            continue;
                        }
                        // fallback 到 summary
                        if (record.getSummary() != null) {
                            chatMemory.add(sessionId, new AssistantMessage(record.getSummary()));
                            episodicUsedCount++;
                            continue;
                        }
                    }
                    chatMemory.add(sessionId, new AssistantMessage(record.getAnswer()));
                }
            }
            log.debug("加载会话历史: sessionId={}, recordCount={}, 使用事件记忆轮次={}", sessionId, history.size(), episodicUsedCount);
        }
        return chatMemory;
    }

    /**
     * 从 episodic_memory 加载事件并格式化为上下文文本
     */
    private String loadEpisodicContext(String sessionId) {
        try {
            List<EpisodicEvent> events = episodicMemoryService.findBySessionId(sessionId);
            if (events == null || events.isEmpty()) {
                return null;
            }
            StringBuilder sb = new StringBuilder("[历史事件]\n");
            for (EpisodicEvent event : events) {
                switch (event.getEventType()) {
                    case "TOPIC" -> sb.append("- 研究主题：").append(event.getContent()).append("\n");
                    case "FINDING" -> sb.append("- 发现：").append(event.getContent()).append("\n");
                    case "DIMENSION" -> sb.append("- 覆盖维度：").append(event.getContent()).append("\n");
                    case "FAILURE" -> sb.append("- 未解决：").append(event.getContent()).append("\n");
                    default -> sb.append("- ").append(event.getContent()).append("\n");
                }
            }
            return sb.toString().trim();
        } catch (Exception e) {
            log.warn("加载事件记忆失败: sessionId={}", sessionId, e);
            return null;
        }
    }

    protected void recordFirstResponse() {
        if (firstResponseTime == 0 && startTime > 0) {
            firstResponseTime = System.currentTimeMillis() - startTime;
            log.debug("记录首次响应时间: {}ms", firstResponseTime);
        }
    }

    /**
     * 获取历史消息列表
     *
     * @param conversationId 会话ID
     * @return 历史消息列表
     */
    protected List<Message> getChatHistory(String conversationId) {
        if (conversationId != null && chatMemory != null) {
            return chatMemory.get(conversationId);
        }
        return null;
    }


    protected void recordUsedTool(String toolName) {
        if (usedTools != null && toolName != null) {
            usedTools.add(toolName);
        }
    }

    protected String createResponse(String content, String type) {
        return AgentResponse.json(type, content);
    }

    protected String createTextResponse(String content) {
        return AgentResponse.text(content);
    }

    protected String createThinkingResponse(String content) {
        return AgentResponse.thinking(content);
    }

    protected String createReferenceResponse(String content) {
        return AgentResponse.reference(content);
    }

    protected String createErrorResponse(String content) {
        return AgentResponse.error(content);
    }

    protected String createRecommendResponse(String content) {
        return AgentResponse.recommend(content);
    }



}
