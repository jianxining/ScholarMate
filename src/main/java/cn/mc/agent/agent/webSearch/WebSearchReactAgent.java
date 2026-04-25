package cn.mc.agent.agent.webSearch;


import cn.mc.agent.agent.BaseAgent;
import cn.mc.agent.entity.AiSession;
import cn.mc.agent.entity.record.AgentState;
import cn.mc.agent.entity.record.RoundMode;
import cn.mc.agent.entity.record.RoundState;
import cn.mc.agent.entity.record.SearchResult;
import cn.mc.agent.entity.request.SaveQuestionRequest;
import cn.mc.agent.entity.request.UpdateAnswerRequest;
import cn.mc.agent.prompts.ReactAgentPrompts;
import cn.mc.agent.service.AgentTaskManager;
import cn.mc.agent.service.AiSessionService;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.api.Advisor;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.messages.*;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.model.tool.ToolCallingChatOptions;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.util.CollectionUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author tmengchun
 * @description
 * @create 2026/4/24 11:23
 */

@Slf4j
public class WebSearchReactAgent extends BaseAgent {

    private ChatClient chatClient;
    private final List<ToolCallback> tools;
    private final String systemPrompt;
    private final String description = "WebSearchReactAgent是一个基于React框架设计的智能代理，专门用于处理与网页搜索相关的任务。它能够理解用户的查询意图，利用内置的工具进行信息检索，并通过多轮对话不断优化搜索结果。该Agent还具备反思能力，可以在执行过程中评估自己的表现，并根据反馈进行调整，以提供更准确和相关的搜索结果。";
    private int maxRounds;
    private final List<Advisor> advisors;
    private final int maxReflectionRounds;

    // TODO:ObjectMapper的作用是什么
    private static final ObjectMapper MAPPER = new ObjectMapper();



    public WebSearchReactAgent(String name, ChatModel chatModel, List<ToolCallback> tools, String systemPrompt, int maxRounds,
                               ChatMemory chatMemory, List<Advisor> advisors, int maxReflectionRounds,
                               AiSessionService sessionService, AgentTaskManager taskManager) {
        super(chatModel, name, "websearch");
        this.tools = tools;
        this.systemPrompt = systemPrompt;
        this.maxRounds = maxRounds;
        this.advisors = advisors;
        this.maxReflectionRounds = maxReflectionRounds;
        this.chatMemory = chatMemory;
        this.sessionService = sessionService;
        this.taskManager = taskManager;

        // 初始化工具记录集合
        this.usedTools = new HashSet<>();
        initChatClient();

        if (this.chatClient == null) {
            throw new IllegalStateException("ChatClient 初始化失败！");
        }
    }

    private void initChatClient() {
        try {
            ToolCallingChatOptions toolOptions = ToolCallingChatOptions.builder()
                    .toolCallbacks(tools)
                    .internalToolExecutionEnabled(false)
                    .build();

            ChatClient.Builder builder = ChatClient.builder(chatModel);
            if (!CollectionUtils.isEmpty(advisors)) {
                builder.defaultAdvisors(advisors);
            }
            this.chatClient = builder.defaultOptions(toolOptions).defaultToolCallbacks(tools).build();
        } catch (Exception e) {
            throw new RuntimeException("ChatClient 初始化失败：" + e.getMessage(), e);
        }
    }

    @Override
    public Flux<String> execute(String input, String conversationId) {
        return streamInternal(conversationId, input);
    }

    public Flux<String> stream(String conversationId, String input) {
        return streamInternal(conversationId, input);
    }

    public Flux<String> streamInternal(String conversationId, String input) {
        // synchronizedList 用于保证多线程环境下对消息列表的安全访问
        List<Message> messages = Collections.synchronizedList(new ArrayList<>());
        // 判断当前是否需要记忆,如果conversationId不为空且chatMemory可用，则启用记忆功能
        boolean useMemory = conversationId != null && chatMemory != null;

        // 对于同一个conversationId的请求，先检查是否有正在运行的任务，如果有则拒绝新的请求，避免重复执行
        Flux<String> checkResult = checkRunningTask(conversationId);
        if (checkResult != null) {
            return checkResult;
        }

        // 正式进入执行流程
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();

        initTimers(); // 初始化计时器
        clearUsedTools(); // 清除已使用工具记录


        AgentTaskManager.TaskInfo taskInfo = registerTask(conversationId, sink);
        if (taskInfo == null && conversationId != null && taskManager != null) {
            return Flux.error(new IllegalStateException("无法注册任务，conversationId=" + conversationId + "可能已经有正在运行的任务"));
        }

        // ====构建系统Prompt====
        messages.add(new SystemMessage(ReactAgentPrompts.getWebSearchPrompt()));
        if (StringUtils.isNotBlank(systemPrompt)) {
            messages.add(new SystemMessage(systemPrompt));
        }

        // ====构建历史对话====
        loadChatHistory(conversationId, messages,true,true);

        // ====构建用户输入====
        messages.add(new UserMessage("<question>" + input + "</question>"));
        currentQuestion = input;

        // 添加记忆并保存到数据库
        if (sessionService != null) {
            // 保存用户问题到数据库
            AiSession savedSession = sessionService.saveQuestion(
                    SaveQuestionRequest.builder()
                            .sessionId(conversationId)
                            .question(input)
                            .build()
            );
            currentSessionId = savedSession.getId();
        }

        // 迭代轮次
        AtomicLong roundCounter = new AtomicLong(0);
        // 是否发送最终结果标记位
        AtomicBoolean hasSentFinalResult = new AtomicBoolean(false);

        hasSentFinalResult.set(false);
        roundCounter.set(0);

        // 收集最终答案（纯文本），存储memory
        StringBuilder finalAnswerBuffer = new StringBuilder();
        // 收集思考过程
        StringBuilder thinkingBuffer = new StringBuilder();

        AgentState agentState = new AgentState();
        scheduleRound(messages, sink, roundCounter, hasSentFinalResult, finalAnswerBuffer, useMemory, conversationId, agentState, thinkingBuffer);
        return sink.asFlux()
                .doOnNext(chunk->{
                    recordFirstResponse(); // 记录首次响应时间
                    try {
                        // 解析响应块，提取文本内容并追加到finalAnswerBuffer中
                        JsonNode node = MAPPER.readTree(chunk);
                        String type = node.get("type").asText();
                        String content = node.get("content").asText();
                        if ("text".equals(type)) {
                            finalAnswerBuffer.append(content);
                        } else if ("thinking".equals(type)) {
                            thinkingBuffer.append(content);
                        }
                    } catch (Exception e) {
                        log.warn("解析响应块失败: {}", e.getMessage());
                    }
                })
                .doOnCancel(()->{
                    // 取消订阅时的清理工作，例如取消正在执行的工具调用等
                   hasSentFinalResult.set(true);
                   if (taskManager != null) {
                       taskManager.stopTask(conversationId);
                   }
                })
                .doFinally(signalType -> {
                    // 流结束的清理工作和记录日志等
                    log.info("最终答案:{}",finalAnswerBuffer);
                    log.info("思考过程:{}",thinkingBuffer);
                     if (conversationId != null && sessionService != null) {
                         saveSessionResult(conversationId, finalAnswerBuffer, thinkingBuffer, agentState);
                     }
                     if (taskManager != null) {
                         taskManager.stopTask(conversationId);
                     }
                });
    }

    private void scheduleRound(List<Message> messages, Sinks.Many<String> sink, AtomicLong roundCounter, AtomicBoolean hasSentFinalResult,
                          StringBuilder finalAnswerBuffer, boolean useMemory, String conversationId, AgentState agentState, StringBuilder thinkingBuffer) {
        roundCounter.incrementAndGet();
        RoundState state = new RoundState();
        Disposable subscribe = chatClient.prompt()
                .messages(messages)
                .stream()
                .chatResponse()
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(chunk -> processChunk(chunk, sink,state))
                .doOnComplete(() -> finishRound(messages, sink, state, roundCounter, hasSentFinalResult, finalAnswerBuffer, useMemory, conversationId, agentState, thinkingBuffer))
                .doOnError(err ->{
                     hasSentFinalResult.set(true);
                     sink.tryEmitError(err);
                })
                .subscribe();
        if (conversationId != null && taskManager != null) {
            taskManager.setDisposable(conversationId,subscribe);
        }
    }

    private void processChunk(ChatResponse chunk, Sinks.Many<String> sink, RoundState state){
        // 处理每个响应块的逻辑，包括更新状态、记录工具调用、判断是否完成等
        if (chunk == null) {
            return;
        }
        Generation generation = chunk.getResult();
        String text = generation.getOutput().getText();
        List<AssistantMessage.ToolCall> tc = generation.getOutput().getToolCalls();
        // 一旦发现 tool_call，立即进入 TOOL_CALL 模式
        if (tc != null && !tc.isEmpty()) {
            state.mode = RoundMode.TOOL_CALL;
            for (AssistantMessage.ToolCall incoming : tc) {
                mergeToolCall(state, incoming);
            }
            return;
        }
        if (text != null) {
            sink.tryEmitNext(createTextResponse(text));
            state.textBuffer.append(text);
        }
    }

    // 模型流式输出的时候，可能会将一次工具调用的信息分成多块返回，目前观察到的现象是function不会缺失，但arguments可能会被拆分成多块返回，因此需要合并处理，确保工具调用信息的完整性
    private void mergeToolCall(RoundState state, AssistantMessage.ToolCall incoming) {
        // 针对相同的ToolCallID，合并处理其的调用信息，避免重复记录和参数缺失
        for (int i=0;i<state.toolCalls.size();i++) {
            AssistantMessage.ToolCall existing = state.toolCalls.get(i);
            if (existing.id().equals(incoming.id())) {
                // 说明是同一次工具调用的不同块信息，进行合并处理
                String mergedArguments = existing.arguments() + incoming.arguments();
                state.toolCalls.set(i,new AssistantMessage.ToolCall(existing.id(), "function", existing.name(), mergedArguments));
                return;
            }
        }
        // 如果没有找到相同ID的ToolCall，说明是新的工具调用，直接添加到列表中
        state.toolCalls.add(incoming);
    }

    /**
     * 轮次结束处理工具调用
     *
     */
    private void finishRound(List<Message> messages, Sinks.Many<String> sink, RoundState state,
                             AtomicLong roundCounter, AtomicBoolean hasSentFinalResult, StringBuilder finalAnswerBuffer,
                             boolean useMemory, String conversationId, AgentState agentState, StringBuilder thinkingBuffer) {
        // 轮次结束后，首先判断是否有工具调用，如果有则优先处理工具调用逻辑
        if (state.mode != RoundMode.TOOL_CALL) {
            normalFinalStream(state, sink, conversationId, agentState, hasSentFinalResult);
            return;
        }
        // 处理工具调用逻辑
        // ===== 保存工具调用信息到上下文里 =====
        AssistantMessage assistantMessage = AssistantMessage.builder().toolCalls(state.getToolCalls()).build();
        messages.add(assistantMessage);

        // 判断是否达到最大轮次限制，如果达到则结束对话并返回结果
        if (maxRounds > 0 && roundCounter.get() >= maxRounds) {
            forceFinalStream(messages, sink, hasSentFinalResult, state, conversationId, useMemory, agentState, thinkingBuffer);
            return;
        }

        // 执行工具
        executeToolCalls(sink, state.toolCalls,messages, hasSentFinalResult, state,agentState,()->{
            if (!hasSentFinalResult.get()) {
                // 执行新的一轮scheduleRound，继续对话
                scheduleRound(messages, sink, roundCounter, hasSentFinalResult, finalAnswerBuffer,useMemory, conversationId, agentState, thinkingBuffer);
            }
        });
    }


    private void normalFinalStream(RoundState state, Sinks.Many<String> sink, String conversationId, AgentState agentState, AtomicBoolean hasSentFinalResult) {
        String referenceJson = "";
        String toolsStr = getUsedToolsString();
        String finalText = state.textBuffer.toString();

        // 输出参考链接
        if (!agentState.searchResults.isEmpty()) {
            String reference = com.alibaba.fastjson2.JSON.toJSONString(agentState.searchResults);
            referenceJson = createReferenceResponse(reference);
            sink.tryEmitNext(referenceJson);
        }

        // 输出推荐问题
        if (enableRecommendations) {
            String recommendations = generateRecommendations(conversationId, currentQuestion, finalText);
            if (recommendations != null) {
                currentRecommendations = recommendations; // 保存用于数据库存储
                String recommendJson = createRecommendResponse(recommendations);
                sink.tryEmitNext(recommendJson);
            }
        }

        sink.tryEmitComplete();
        hasSentFinalResult.set(true);
    }

    private void forceFinalStream(List<Message> messages, Sinks.Many<String> sink, AtomicBoolean hasSentFinalResult, RoundState state,
                                  String conversationId, boolean useMemory, AgentState agentState, StringBuilder thinkingBuffer) {
        // 进入总结阶段，发送总结提示语并结束流
        // 创建新的消息列表，确保系统提示词在最前面
        List<Message> newMessages = new ArrayList<>();

        // 添加系统提示词
        newMessages.add(new SystemMessage(ReactAgentPrompts.getWebSearchPrompt()));
        if (StringUtils.isNotBlank(systemPrompt)) {
            newMessages.add(new SystemMessage(systemPrompt));
        }

        // 添加原有消息（跳过系统消息）
        for (Message msg : messages) {
            if (!(msg instanceof SystemMessage)) {
                newMessages.add(msg);
            }
        }

        // 添加总结提示语 UserMessage
        newMessages.add(new UserMessage("""
                你已达到最大推理轮次限制。
                请基于当前已有的上下文信息，
                直接给出最终答案。
                禁止再调用任何工具。
                如果信息不完整，请合理总结和说明。
                """));

        // 替换原消息列表
        messages.clear();
        messages.addAll(newMessages);

        // 收集最终的文本
        StringBuilder finalTextBuffer = new StringBuilder();

        Disposable disposable = chatClient.prompt()
                .messages(messages)
                .stream()
                .chatResponse()
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(chunk -> {
                    if (chunk != null) {
                        Generation generation = chunk.getResult();
                        String text = generation.getOutput().getText();
                        if (text != null) {
                            finalTextBuffer.append(text);
                            sink.tryEmitNext(createTextResponse(text));
                        }
                    }
                })
                .doOnComplete(() -> {
                    // 发送最终答案
                    normalFinalStream(state,sink, conversationId, agentState, hasSentFinalResult);
                })
                .doOnError(
                        err -> {
                           hasSentFinalResult.set(true);
                           sink.tryEmitError(err);
                        }
                )
                .subscribe();

        // 保存Disposable到任务管理器
        if (conversationId != null && taskManager != null) {
            taskManager.setDisposable(conversationId, disposable);
        }
    }

    private void executeToolCalls(Sinks.Many<String> sink, List<AssistantMessage.ToolCall> toolCalls, List<Message> messages,
                              AtomicBoolean hasSentFinalResult, RoundState state, AgentState agentState, Runnable onComplete) {

        AtomicInteger completedCount = new AtomicInteger(0);
        int totalToolCalls = toolCalls.size();

        Map<String, ToolResponseMessage.ToolResponse> responseMap = new ConcurrentHashMap<>();
        for (AssistantMessage.ToolCall tc:toolCalls) {
            // Schedulers.boundedElastic()适合执行阻塞的工具调用，避免占用chatClient所在的线程池资源
            // TODO:Schedulers.boundedElastic()是什么样的线程池？为什么适合执行工具调用？是否需要考虑工具调用的数量和频率，避免过多的工具调用导致线程池资源耗尽？
            Schedulers.boundedElastic().schedule(()->{
                if (hasSentFinalResult.get()) {
                    // 如果已经发送最终结果，则不再执行工具调用，直接返回
                    completeToolCall(completedCount,totalToolCalls,responseMap, toolCalls, messages, onComplete);
                    return;
                }
                // 执行工具调用的逻辑，处理结果并更新状态

                String toolName = tc.name();
                String argsJson = tc.arguments();
                ToolCallback callback = findTool(toolName);
                if (callback == null) { // 没有找到对应的工具
                    responseMap.put(tc.id(), new ToolResponseMessage.ToolResponse(tc.id(), toolName,  "{\"error\" :\"未找到工具：" + toolName + "\"}"));
                    // TODO:为什么这里需要执行completeToolCall？
                    completeToolCall(completedCount,totalToolCalls,responseMap, toolCalls, messages, onComplete);
                    return;
                }

                // ===== 执行工具 =====

                if (toolName.contains("search")) {
                    JSONObject args = JSON.parseObject(argsJson);
                    String query = (String)args.get("query");
                    String queryThink = StringUtils.isNotBlank(query) ? "正在搜索信息" + query  + "\n" : "正在搜索相关信息\n";
                    sink.tryEmitNext(createThinkingResponse(queryThink));
                    String result = callback.call(argsJson);
                    try {
                        recordUsedTool(toolName);
                        // 解析搜索结果
                        if (toolName.contains("tavily")) {
                            parseSearchResult(result, agentState);
                        }
                        responseMap.put(tc.id(), new ToolResponseMessage.ToolResponse(tc.id(), toolName, result));
                    }catch (Exception ex) {
                        responseMap.put(tc.id(), new ToolResponseMessage.ToolResponse(tc.id(), toolName,  "{\"error\" :\"工具执行失败：" + ex.getMessage() + "\"}"));
                    }finally {
                        completeToolCall(completedCount,totalToolCalls,responseMap, toolCalls, messages, onComplete);
                    }
                }
            });
        }
    }

    // 完成单轮工具调用后，检查是否所有工具调用都已完成，如果完成则将结果按原始顺序添加到消息列表，并执行后续操作
    private void completeToolCall(AtomicInteger completedCount, int total,
                                  Map<String, ToolResponseMessage.ToolResponse> responseMap,
                                  List<AssistantMessage.ToolCall> originalToolCalls,
                                  List<Message> messages,
                                  Runnable onComplete) {
        int current = completedCount.incrementAndGet();
        if (current >= total) {
            // 按原始 toolCalls 的顺序重组结果
            List<ToolResponseMessage.ToolResponse> sortedResponses = new ArrayList<>();
            for (AssistantMessage.ToolCall tc : originalToolCalls) {
                ToolResponseMessage.ToolResponse response = responseMap.get(tc.id());
                if (response != null) {
                    sortedResponses.add(response);
                } else {
                    // 如果某个工具调用没有响应，添加一个错误响应
                    sortedResponses.add(new ToolResponseMessage.ToolResponse(
                            tc.id(), tc.name(), "{ \"error\": \"工具响应丢失\" }"));
                }
            }

            // 一次性添加所有工具响应（按原始顺序）
            messages.add(ToolResponseMessage.builder()
                    .responses(sortedResponses)
                    .build());
            onComplete.run();
        }
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


    protected Flux<String> checkRunningTask(String conversationId) {
        if (conversationId != null && taskManager != null && taskManager.hasRunningTask(conversationId)) {
            return Flux.error(new IllegalStateException("该会话正在执行中，请稍后再试"));
        }
        return null;
    }


    private ToolCallback findTool(String name) {
        return tools.stream()
                .filter(t -> t.getToolDefinition().name().equals(name))
                .findFirst()
                .orElse(null);
    }

    private void saveSessionResult(String conversationId, StringBuilder finalAnswerBuffer, StringBuilder thinkingBuffer, AgentState agentState) {
        if (sessionService != null && currentSessionId != null && finalAnswerBuffer.length() > 0) {
            long totalResponseTime = getTotalResponseTime();
            String toolsStr = getUsedToolsString();
            String referenceJson = "";
            if (!agentState.searchResults.isEmpty()) {
                referenceJson = createReferenceResponse(JSON.toJSONString(agentState.searchResults));
            }
            UpdateAnswerRequest request = UpdateAnswerRequest.builder()
                    .id(currentSessionId)
                    .answer(finalAnswerBuffer.toString())
                    .thinking(thinkingBuffer.toString())
                    .tools(toolsStr)
                    .reference(referenceJson)
                    .recommend(currentRecommendations)
                    .firstResponseTime(firstResponseTime)
                    .totalResponseTime(totalResponseTime)
                    .build();
            sessionService.updateAnswer(request);
            log.info("结果已保存到会话: sessionId={}", conversationId);
        }
    }

    private void parseSearchResult(String resultJson, AgentState state) {
        try {
            JsonNode root = MAPPER.readTree(resultJson);

            if (!root.isArray() || root.isEmpty()) {
                return;
            }

            JsonNode first = root.get(0);
            JsonNode textNode = first.get("text");

            if (textNode == null || textNode.isNull()) {
                return;
            }

            JsonNode textJson;
            if (textNode.isTextual()) {
                textJson = MAPPER.readTree(textNode.asText());
            } else {
                textJson = textNode;
            }

            JsonNode results = textJson.get("results");
            if (results == null || !results.isArray()) {
                return;
            }

            for (JsonNode item : results) {
                String url = getSafe(item, "url");
                String title = getSafe(item, "title");
                String content = getSafe(item, "content");

                if (url != null && !url.isBlank()) {
                    state.searchResults.add(new SearchResult(url, title, content));
                }
            }

        } catch (Exception e) {
            log.warn("解析 tavily 搜索结果失败: {}", e.getMessage());
        }
    }

    private String getSafe(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    public static Builder builder(){
        return new Builder();
    }

    public static class Builder {
        private String name;
        private ChatModel chatModel;
        private List<ToolCallback> tools;
        private String systemPrompt = "";
        private int maxReflectionRounds;
        private int maxRounds;
        private List<Advisor> advisors;
        private ChatMemory chatMemory;
        private AiSessionService sessionService;
        private AgentTaskManager taskManager;

        public Builder chatMemory(ChatMemory chatMemory) {
            this.chatMemory = chatMemory;
            return this;
        }

        public Builder sessionService(AiSessionService sessionService) {
            this.sessionService = sessionService;
            return this;
        }

        public Builder taskManager(AgentTaskManager taskManager) {
            this.taskManager = taskManager;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder chatModel(ChatModel chatModel) {
            this.chatModel = chatModel;
            return this;
        }

        public Builder tools(ToolCallback... tools) {
            this.tools = Arrays.asList(tools);
            return this;
        }

        public Builder tools(List<ToolCallback> tools) {
            this.tools = tools;
            return this;
        }

        public Builder advisors(List<Advisor> advisors) {
            this.advisors = advisors;
            return this;
        }

        public Builder advisors(Advisor... advisors) {
            this.advisors = Arrays.asList(advisors);
            return this;
        }

        public Builder systemPrompt(String systemPrompt) {
            this.systemPrompt = systemPrompt;
            return this;
        }

        public Builder maxReflectionRounds(int maxReflectionRounds) {
            this.maxReflectionRounds = maxReflectionRounds;
            return this;
        }

        public Builder maxRounds(int maxRounds) {
            this.maxRounds = maxRounds;
            return this;
        }

        public WebSearchReactAgent build() {
            if (chatModel == null) {
                throw new IllegalArgumentException("chatModel 不能为空！");
            }
            return new WebSearchReactAgent(name, chatModel, tools, systemPrompt, maxRounds, chatMemory, advisors, maxReflectionRounds, sessionService, taskManager);
        }
    }


}
