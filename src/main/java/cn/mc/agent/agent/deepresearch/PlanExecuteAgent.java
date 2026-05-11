package cn.mc.agent.agent.deepresearch;

import cn.mc.agent.agent.BaseAgent;
import cn.mc.agent.entity.AiSession;
import cn.mc.agent.entity.OverAllState;
import cn.mc.agent.entity.record.CritiqueResult;
import cn.mc.agent.entity.record.PlanTask;
import cn.mc.agent.entity.record.SearchResult;
import cn.mc.agent.entity.record.TaskResult;
import cn.mc.agent.entity.record.TaskStatus;
import cn.mc.agent.entity.request.SaveQuestionRequest;
import cn.mc.agent.entity.request.UpdateAnswerRequest;
import cn.mc.agent.entity.response.SimpleReactResult;
import cn.mc.agent.prompts.PlanExecutePrompts;
import cn.mc.agent.service.AgentTaskManager;
import cn.mc.agent.service.AiSessionService;
import cn.mc.agent.service.MemoryExtractionService;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.converter.BeanOutputConverter;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.core.ParameterizedTypeReference;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


@Slf4j
public class PlanExecuteAgent extends BaseAgent {

    private ChatClient chatClient;
    private final List<ToolCallback> tools;

    // plan-execute 总轮数
    private final int maxRounds;

    // context 压缩阈值
    private final int contextCharLimit;

    // 控制工具并发调用上限
    private final Semaphore toolSemaphore;

    // 工具重试次数
    private final int maxToolRetries;

    // 用于管理所有需要取消的Disposable
    private Disposable.Composite compositeDisposable;

    // 存储所有搜索结果，用于保存到数据库和发送给前端
    private List<SearchResult> allReferences;

    // 统一记忆提取服务
    private MemoryExtractionService memoryExtractionService;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public PlanExecuteAgent(ChatModel chatModel,
                            List<ToolCallback> tools,
                            int maxRounds,
                            int contextCharLimit,
                            int maxToolRetries,
                            ChatMemory chatMemory,
                            AiSessionService sessionService,
                            AgentTaskManager taskManager,
                            MemoryExtractionService memoryExtractionService) {
        super(chatModel, "PlanExecuteAgent", "plan-execute");
        this.chatClient = ChatClient.builder(chatModel).build();
        this.tools = tools;
        this.maxRounds = maxRounds;
        this.contextCharLimit = contextCharLimit;
        this.maxToolRetries = maxToolRetries;
        this.toolSemaphore = new Semaphore(3);
        this.chatMemory = chatMemory;
        this.sessionService = sessionService;
        this.taskManager = taskManager;
        this.memoryExtractionService = memoryExtractionService;

        // 初始化工具记录集合
        this.usedTools = new HashSet<>();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ChatModel chatModel;
        private List<ToolCallback> tools = new ArrayList<>();

        // 默认迭代3轮
        private int maxRounds = 3;

        // 默认context压缩阈值20000字符
        private int contextCharLimit = 50000;

        // 默认工具重试次数2次
        private int maxToolRetries = 2;

        private ChatMemory chatMemory;

        private AiSessionService sessionService;

        private AgentTaskManager taskManager;

        private MemoryExtractionService memoryExtractionService;

        public Builder sessionService(AiSessionService sessionService) {
            this.sessionService = sessionService;
            return this;
        }

        public Builder taskManager(AgentTaskManager taskManager) {
            this.taskManager = taskManager;
            return this;
        }

        public Builder memoryExtractionService(MemoryExtractionService memoryExtractionService) {
            this.memoryExtractionService = memoryExtractionService;
            return this;
        }

        public Builder chatMemory(ChatMemory chatMemory) {
            this.chatMemory = chatMemory;
            return this;
        }

        public Builder chatModel(ChatModel chatModel) {
            this.chatModel = chatModel;
            return this;
        }

        public Builder tools(List<ToolCallback> tools) {
            this.tools = tools;
            return this;
        }

        public Builder tools(ToolCallback... tools) {
            this.tools = Arrays.asList(tools);
            return this;
        }

        public Builder maxRounds(int maxRounds) {
            this.maxRounds = maxRounds;
            return this;
        }

        public Builder contextCharLimit(int contextCharLimit) {
            this.contextCharLimit = contextCharLimit;
            return this;
        }

        public Builder maxToolRetries(int maxToolRetries) {
            this.maxToolRetries = maxToolRetries;
            return this;
        }

        public PlanExecuteAgent build() {
            Objects.requireNonNull(chatModel, "chatModel must not be null");
            return new PlanExecuteAgent(chatModel, tools, maxRounds, contextCharLimit, maxToolRetries, chatMemory, sessionService, taskManager, memoryExtractionService);
        }
    }

    public String call(String question) {
        return callInternal(null, question).blockLast();
    }

    public String call(String conversationId, String question) {
        return callInternal(conversationId, question).blockLast();
    }

    public Flux<String> stream(String question) {
        return callInternal(null, question);
    }

    public Flux<String> stream(String conversationId, String question) {
        return callInternal(conversationId, question);
    }

    @Override
    public Flux<String> execute(String conversationId, String question) {
        return callInternal(conversationId, question);
    }

    public Flux<String> callInternal(String conversationId, String question) {

        // 检查是否已有任务在执行
        Flux<String> checkResult = checkRunningTask(conversationId);
        if (checkResult != null) {
            return checkResult;
        }

        // 初始化状态和缓冲区
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();
        AtomicBoolean finished = new AtomicBoolean(false);

        // 注册任务到管理器
        if (!registerTaskInternal(conversationId, sink)) {
            return Flux.error(new IllegalStateException("该会话正在执行中，请稍后再试"));
        }

        // 初始化会话信息
        initTimers();
        clearUsedTools();
        currentConversationId = conversationId;
        currentQuestion = question;
        compositeDisposable = Disposables.composite();

        // 创建缓冲区
        StringBuilder finalAnswerBuffer = new StringBuilder();
        StringBuilder thinkingBuffer = new StringBuilder();
        allReferences = new ArrayList<>();

        // 初始化状态并保存问题
        OverAllState state = initStateAndSaveQuestion(conversationId, question);

        // 启动流程：需求澄清 -> 研究主题生成 -> 执行循环
        clarifyRequirementPhase(state, sink, finished, thinkingBuffer,
                () -> generateResearchTopicPhase(state, sink, finished, thinkingBuffer,
                        () -> executeLoopPhase(state, sink, finished, finalAnswerBuffer,
                                thinkingBuffer)));

        // 注册任务到管理器
        registerTaskToManager(conversationId);

        return wrapSinkWithHandlers(sink, finished, conversationId, finalAnswerBuffer, thinkingBuffer);
    }

    /**
     * 初始化状态并保存问题到数据库
     */
    private OverAllState initStateAndSaveQuestion(String conversationId, String question) {
        OverAllState state = new OverAllState(conversationId, question);

        // 加载历史消息
        List<Message> history = getChatHistory(conversationId);
        if (CollectionUtils.isNotEmpty(history)) {
            history.forEach(state::add);
        }
        state.add(new UserMessage(question));

        // 保存用户问题到数据库
        if (conversationId != null && sessionService != null) {
            AiSession savedSession = sessionService.saveQuestion(
                    SaveQuestionRequest.builder()
                            .sessionId(conversationId)
                            .question(question)
                            .firstResponseTime(firstResponseTime)
                            .build()
            );
            currentSessionId = savedSession.getId();
        }

        return state;
    }

    /**
     * 注册任务到管理器（内部方法）
     */
    private boolean registerTaskInternal(String conversationId, Sinks.Many<String> sink) {
        if (conversationId == null) {
            return true;
        }
        AgentTaskManager.TaskInfo taskInfo = registerTask(conversationId, sink);
        return taskInfo != null || taskManager == null;
    }

    /**
     * 注册任务到任务管理器
     */
    private void registerTaskToManager(String conversationId) {
        if (conversationId != null && taskManager != null) {
            taskManager.setDisposable(conversationId, compositeDisposable);
        }
    }

    /**
     * 包装 sink 并添加处理逻辑
     */
    private Flux<String> wrapSinkWithHandlers(Sinks.Many<String> sink, AtomicBoolean finished,
                                              String conversationId, StringBuilder finalAnswerBuffer,
                                              StringBuilder thinkingBuffer) {
        return sink.asFlux()
                .doOnNext(chunk -> {
                    recordFirstResponse();
                    parseAndAppendToBuffers(chunk, finalAnswerBuffer, thinkingBuffer);
                })
                .doOnCancel(() -> handleCancel(sink, finished))
                .doFinally(signalType -> handleFinally(signalType, conversationId, finalAnswerBuffer,
                        thinkingBuffer, finished));
    }

    /**
     * 解析消息并追加到对应的缓冲区
     */
    private void parseAndAppendToBuffers(String chunk, StringBuilder finalAnswerBuffer, StringBuilder thinkingBuffer) {
        try {
            JSONObject json = JSON.parseObject(chunk);
            String type = json.getString("type");
            if ("text".equals(type)) {
                finalAnswerBuffer.append(json.getString("content"));
            } else if ("thinking".equals(type)) {
                thinkingBuffer.append(json.getString("content"));
            }
        } catch (Exception e) {
            // 解析失败，默认为 text 类型
            finalAnswerBuffer.append(chunk);
        }
    }

    /**
     * 处理取消操作
     */
    private void handleCancel(Sinks.Many<String> sink, AtomicBoolean finished) {
        finished.set(true);
        taskManager.stopTask(currentConversationId);
    }

    /**
     * 处理流结束
     */
    private void handleFinally(reactor.core.publisher.SignalType signalType, String conversationId,
                               StringBuilder finalAnswerBuffer, StringBuilder thinkingBuffer,
                               AtomicBoolean finished) {
        log.info("流结束，类型: {}, 最终答案长度: {}, 思考过程长度: {}",
                signalType, finalAnswerBuffer.length(), thinkingBuffer.length());

        // 保存结果到会话
        saveSessionResult(conversationId, finalAnswerBuffer, thinkingBuffer);

        // 移除任务
        taskManager.stopTask(conversationId);

        // 清理资源
        cleanupResources(finished);
    }

    /**
     * 清理资源
     */
    private void cleanupResources(AtomicBoolean finished) {
        if (compositeDisposable != null && !compositeDisposable.isDisposed()) {
            compositeDisposable.dispose();
        }
        if (finished != null) {
            finished.set(true);
        }
    }

    /**
     * 需求澄清阶段
     */
    private void clarifyRequirementPhase(OverAllState state, Sinks.Many<String> sink,
                                         AtomicBoolean finished, StringBuilder thinkingBuffer, Runnable onComplete) {
        emit(sink, finished, "\n🔍 正在分析您的需求...\n", "thinking", thinkingBuffer);

        List<Message> messages = new ArrayList<>();
        // 先注入时间信息
        messages.add(new SystemMessage(PlanExecutePrompts.getCurrentTime()));
        // 再注入提示词
        messages.add(new SystemMessage(PlanExecutePrompts.REQUIREMENT_CLARIFICATION));
        messages.addAll(state.getMessages());

        StringBuilder responseBuffer = new StringBuilder();

        Disposable disposable = chatClient.prompt()
                .messages(messages)
                .stream()
                .content()
                .doOnNext(chunk -> {
                    responseBuffer.append(chunk);
                    emit(sink, finished, chunk, "thinking", thinkingBuffer);
                })
                .doOnComplete(() -> handleClarificationComplete(responseBuffer, sink, finished,
                        thinkingBuffer, onComplete))
                .doOnError(err -> handleError("需求澄清异常", err, sink, finished))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        compositeDisposable.add(disposable);
    }

    /**
     * 处理需求澄清完成
     */
    private void handleClarificationComplete(StringBuilder responseBuffer, Sinks.Many<String> sink,
                                             AtomicBoolean finished, StringBuilder thinkingBuffer, Runnable onComplete) {
        String response = responseBuffer.toString();
        emit(sink, finished, "\n✅ 需求分析完成\n", "thinking", thinkingBuffer);

        boolean needsMoreInfo = response.contains("【需要补充信息】");

        if (needsMoreInfo) {
            // 需要补充信息，停止流程
            String pauseMessage = "⏸【暂停深入研究】" + response.replace("【需要补充信息】", "").trim();
            sink.tryEmitNext(createTextResponse(pauseMessage));
            complete(sink, finished);
        } else {
            // 信息充足，继续下一步
            emit(sink, finished, "✅ 信息充足，准备生成研究主题\n", "thinking", thinkingBuffer);
            onComplete.run();
        }
    }

    /**
     * 研究主题生成阶段
     */
    private void generateResearchTopicPhase(OverAllState state, Sinks.Many<String> sink,
                                            AtomicBoolean finished, StringBuilder thinkingBuffer, Runnable onComplete) {
        emit(sink, finished, "📝 正在生成研究主题...\n", "thinking", thinkingBuffer);

        List<Message> messages = new ArrayList<>();
        // 先注入时间信息
        messages.add(new SystemMessage(PlanExecutePrompts.getCurrentTime()));
        // 再注入提示词
        messages.add(new SystemMessage(PlanExecutePrompts.RESEARCH_TOPIC_GENERATION));

        // 添加历史消息和对话上下文
        if (CollectionUtils.isNotEmpty(state.getMessages())) {
            messages.addAll(state.getMessages());
        }

        // 添加用户原始问题
        messages.add(new UserMessage("<original_question>" + state.getQuestion() + "</original_question>"));

        StringBuilder topicBuffer = new StringBuilder();

        Disposable disposable = chatClient.prompt()
                .messages(messages)
                .stream()
                .content()
                .doOnNext(chunk -> {
                    topicBuffer.append(chunk);
                    emit(sink, finished, chunk, "thinking", thinkingBuffer);
                })
                .doOnComplete(() -> handleResearchTopicComplete(topicBuffer, state, sink, finished,
                        thinkingBuffer, onComplete))
                .doOnError(err -> handleError("研究主题生成异常", err, sink, finished))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

        compositeDisposable.add(disposable);
    }

    /**
     * 处理研究主题生成完成
     */
    private void handleResearchTopicComplete(StringBuilder topicBuffer, OverAllState state,
                                             Sinks.Many<String> sink, AtomicBoolean finished,
                                             StringBuilder thinkingBuffer, Runnable onComplete) {
        String topic = topicBuffer.toString();
        state.setRefinedResearchTopic(topic);

        emit(sink, finished, "\n✅ 研究主题已生成\n\n", "thinking", thinkingBuffer);
        onComplete.run();
    }

    /**
     * 执行循环阶段
     */
    private void executeLoopPhase(OverAllState state, Sinks.Many<String> sink,
                                  AtomicBoolean finished, StringBuilder finalAnswerBuffer,
                                  StringBuilder thinkingBuffer) {
        Mono<Void> executionMono = executeLoop(state, sink, finished, finalAnswerBuffer,
                thinkingBuffer);

        Disposable executionDisposable = executionMono.subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        unused -> {
                        },
                        e -> handleExecutionError(e, sink, finished)
                );

        compositeDisposable.add(executionDisposable);
    }

    /**
     * 处理执行过程中的异常
     */
    private void handleExecutionError(Throwable e, Sinks.Many<String> sink, AtomicBoolean finished) {
        // 检查是否是中断导致的异常
        if (compositeDisposable.isDisposed() || Thread.currentThread().isInterrupted()
                || (e.getMessage() != null && e.getMessage().contains("interrupted"))) {
            log.info("PlanExecuteAgent 执行被用户停止: {}", e.getMessage());
        } else {
            log.error("PlanExecuteAgent execute error", e);
            error(sink, finished, e);
        }
    }

    /**
     * 处理错误
     */
    private void handleError(String logMessage, Throwable err, Sinks.Many<String> sink, AtomicBoolean finished) {
        log.error(logMessage, err);
        error(sink, finished, err);
    }

    /**
     * 保存会话结果
     */
    private void saveSessionResult(String conversationId, StringBuilder finalAnswerBuffer, StringBuilder thinkingBuffer) {
        if (sessionService == null || currentSessionId == null) {
            return;
        }

        try {
            // 检查是否有任何内容需要保存
            boolean hasContent = finalAnswerBuffer.length() > 0 || thinkingBuffer.length() > 0;

            if (!hasContent) {
                log.info("没有内容需要保存: conversationId={}", conversationId);
                return;
            }

            long totalResponseTime = getTotalResponseTime();
            String toolsStr = getUsedToolsString();
            String referenceJson = "";
            if (allReferences != null && !allReferences.isEmpty()) {
                referenceJson = createReferenceResponse(JSON.toJSONString(allReferences));
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
            log.info("结果已保存到会话: sessionId={}, conversationId={}", currentSessionId, conversationId);

            // 异步统一记忆提取（不阻塞响应流）
            final Long sessionId = currentSessionId;
            Schedulers.boundedElastic().schedule(() -> {
                try {
                    if (memoryExtractionService != null) {
                        memoryExtractionService.extractAll(sessionId);
                    }
                } catch (Exception e) {
                    log.error("异步记忆提取失败: sessionId={}", sessionId, e);
                }
            });
        } catch (Exception e) {
            log.error("保存结果到会话失败", e);
        }
    }

    /**
     * 发送响应（缓冲区收集由 wrapSinkWithHandlers 统一处理）
     */
    private void emit(Sinks.Many<String> sink,
                      AtomicBoolean finished,
                      String content,
                      String type) {

        if (finished.get()) {
            return;
        }
        sink.tryEmitNext(createResponse(content, type));
    }

    /**
     * 发送响应
     */
    private void emit(Sinks.Many<String> sink,
                      AtomicBoolean finished,
                      String content,
                      String type,
                      StringBuilder thinkingBuffer) {

        if (finished.get()) {
            return;
        }
        sink.tryEmitNext(createResponse(content, type));
    }

    private void complete(Sinks.Many<String> sink,
                          AtomicBoolean finished) {

        if (finished.compareAndSet(false, true)) {
            sink.tryEmitComplete();
        }
    }

    private void error(Sinks.Many<String> sink,
                       AtomicBoolean finished,
                       Throwable e) {

        if (finished.compareAndSet(false, true)) {
            sink.tryEmitError(e);
        }
    }

    private Mono<Void> executeLoop(OverAllState state,
                                   Sinks.Many<String> sink,
                                   AtomicBoolean finished,
                                   StringBuilder finalAnswerBuffer,
                                   StringBuilder thinkingBuffer) {

        return Mono.fromRunnable(() -> {
            try {
                while (state.getRound() < maxRounds && !finished.get() && !compositeDisposable.isDisposed()) {

                    state.nextRound();
                    log.info("===== Plan-Execute Round {} =====", state.getRound());

                    // 输出轮次分隔线
                    emit(sink, finished, "\n🔄 第 " + state.getRound() + " 轮研究开始\n", "thinking", thinkingBuffer);

                    // 生成计划，检测环则自动重试
                    List<PlanTask> plan = null;
                    for (int retry = 0; retry < 3; retry++) {
                        plan = generatePlan(state, sink, finished, thinkingBuffer);
                        if (finished.get() || compositeDisposable.isDisposed()) return;
                        if (plan.isEmpty() || plan.stream().allMatch(t -> t.id() == null)) break;

                        String cycleInfo = detectCycleDetail(plan);
                        if (cycleInfo == null) break;

                        emit(sink, finished, "\n⚠️ 检测到循环依赖，重新规划（第" + (retry + 1) + "次）...\n",
                                "thinking", thinkingBuffer);
                        state.add(new AssistantMessage("""
                                【Plan Error】
                                计划中存在循环依赖：%s
                                请重新生成计划，确保 blockedBy 不形成环。
                                """.formatted(cycleInfo)));
                    }
                    if (plan == null || plan.isEmpty() || plan.stream().allMatch(t -> t.id() == null)) {
                        break;
                    }

                    // 执行计划前的分隔
                    emit(sink, finished, "\n--- 开始执行任务 ---\n\n", "thinking", thinkingBuffer);

                    Map<String, TaskResult> results = executePlan(plan, state, sink, finished, thinkingBuffer);
                    if (finished.get() || compositeDisposable.isDisposed()) {
                        return;
                    }

                    // 执行完成后的分隔
                    emit(sink, finished, "\n--- 任务执行完成 ---\n\n", "thinking", thinkingBuffer);

                    CritiqueResult critique = critique(state, plan, results, sink, finished, thinkingBuffer);
                    if (finished.get() || compositeDisposable.isDisposed()) {
                        return;
                    }

                    if (critique.passed()) {
                        break;
                    }

                    state.add(new AssistantMessage("""
                            【Critique Feedback】
                            %s
                            """.formatted(critique.feedback())));

                    // 下一轮前的分隔
                    emit(sink, finished, "\n--- 准备进入下一轮迭代 ---\n", "thinking", thinkingBuffer);

                    compressIfNeeded(state, sink, finished, thinkingBuffer);
                }

                // 所有轮次完成后的分隔
                emit(sink, finished, "\n✅ 研究阶段完成，准备生成最终报告\n", "thinking", thinkingBuffer);

                summarizeStream(state, sink, finished, finalAnswerBuffer, thinkingBuffer);
            } catch (Exception e) {
                // 检查是否是dispose导致的异常
                if (compositeDisposable.isDisposed() || Thread.currentThread().isInterrupted()
                        || (e.getMessage() != null && e.getMessage().contains("interrupted"))) {
                    log.info("PlanExecuteAgent 执行被用户停止: {}", e.getMessage());
                    // 发送停止消息
                    sink.tryEmitNext("{\"type\":\"text\",\"content\":\"⏹ 用户已停止生成\\n\"}");
                    complete(sink, finished);
                } else {
                    log.error("PlanExecuteAgent 执行异常", e);
                    throw e;
                }
            }
        });
    }

    private List<PlanTask> generatePlan(OverAllState state, Sinks.Many<String> sink, AtomicBoolean hasSentFinal, StringBuilder thinkingBuffer) {
        String toolDesc = renderToolDescriptions();
        BeanOutputConverter<List<PlanTask>> converter = new BeanOutputConverter<>(new ParameterizedTypeReference<>() {
        });

        Prompt prompt = new Prompt(List.of(
                new SystemMessage(PlanExecutePrompts.getCurrentTime()),
                new SystemMessage(PlanExecutePrompts.PLAN + """
                                                ## 当前上下文
                                                当前轮次: %s

                                                ## 可用工具说明（仅用于规划参考）
                                                %s

                                                ## 输出格式
                                                %s
                        """.formatted(state.getRound(), toolDesc, converter.getFormat())),
                new UserMessage("""
                        【研究主题】
                        %s

                        【对话历史】
                        %s

                        ## 重要约束
                        如果会话历史中存在【Critique Feedback】，你必须：
                        1. 仔细分析反馈中指出的不足
                        2. 新的计划必须直接解决这些问题
                        3. 不要重复之前失败的尝试
                        """.formatted(
                        state.getRefinedResearchTopic() != null ? state.getRefinedResearchTopic() : state.getQuestion(),
                        state.renderFullContext()
                ))
        ));

        // 只输出状态，不流式输出计划内容
        emit(sink, hasSentFinal, "📋 正在生成执行计划...\n", "thinking", thinkingBuffer);

        if (hasSentFinal.get() || compositeDisposable.isDisposed()) {
            return new ArrayList<>();
        }

        String json = chatClient.prompt()
                .messages(prompt.getInstructions())
                .call()
                .content();

        List<PlanTask> planTasks = converter.convert(json);

        emit(sink, hasSentFinal, "\n✅ 执行计划已生成，共 " + planTasks.size() + " 个任务\n", "thinking", thinkingBuffer);

        // 将执行计划表格式化为纯文本展示
        if (!planTasks.isEmpty()) {
            StringBuilder planText = new StringBuilder("\n📋 执行计划表：\n");
            for (PlanTask task : planTasks) {
                planText.append(String.format("  🟠 %s \n", task.instruction()));
            }
            emit(sink, hasSentFinal, planText.toString(), "thinking", thinkingBuffer);
        }
        return planTasks;
    }

    private Map<String, TaskResult> executePlan(List<PlanTask> plan, OverAllState state, Sinks.Many<String> sink,
                                                AtomicBoolean hasSentFinal, StringBuilder thinkingBuffer) {

        Map<String, TaskResult> results = new ConcurrentHashMap<>();

        // 1. 构建 DAG：用 mutable DagTask 包装每个 PlanTask
        Map<String, DagTask> dag = new LinkedHashMap<>();
        for (PlanTask task : plan) {
            if (task.id() == null || task.id().isEmpty()) continue;
            dag.put(task.id(), new DagTask(task));
        }

        Map<String, String> accumulatedResults = new ConcurrentHashMap<>();

        // 2. DAG 调度主循环
        while (!dag.isEmpty()) {
            if (hasSentFinal.get() || compositeDisposable.isDisposed()) break;

            // 2a. 发现所有就绪任务
            List<DagTask> readyTasks = dag.values().stream()
                    .filter(DagTask::isReady)
                    .toList();

            if (readyTasks.isEmpty()) {
                log.warn("DAG 死锁：剩余 {} 个任务无法执行", dag.size());
                for (DagTask dt : dag.values()) {
                    results.put(dt.task().id(),
                            new TaskResult(dt.task().id(), false, null, "因上游任务失败而跳过"));
                }
                break;
            }

            // 2b. 标记为 IN_PROGRESS
            readyTasks.forEach(dt -> dt.setStatus(TaskStatus.IN_PROGRESS));

            // 2c. 并行执行就绪任务
            CountDownLatch latch = new CountDownLatch(readyTasks.size());

            for (DagTask dagTask : readyTasks) {
                String dependencyContext = buildDependencyContext(accumulatedResults, dag, dagTask.task());

                Disposable taskDisposable = Mono.fromRunnable(() -> {
                    boolean acquired = false;
                    try {
                        if (compositeDisposable.isDisposed()) return;

                        toolSemaphore.acquire();
                        acquired = true;

                        if (compositeDisposable.isDisposed()) return;

                        TaskResult result = executeWithRetry(dagTask.task(), dependencyContext, sink, hasSentFinal, thinkingBuffer);
                        results.put(dagTask.task().id(), result);

                        if (result.success() && result.output() != null) {
                            accumulatedResults.put(dagTask.task().id(), result.output());
                            dagTask.setStatus(TaskStatus.COMPLETED);
                        } else {
                            dagTask.setStatus(TaskStatus.FAILED);
                        }

                        // 构建任务结果消息
                        StringBuilder resultMessage = new StringBuilder();
                        resultMessage.append("【Completed Task Result】\n");
                        resultMessage.append("taskId: ").append(dagTask.task().id()).append("\n");
                        resultMessage.append("success: ").append(result.success()).append("\n");
                        if (result.output() != null) {
                            resultMessage.append("result:\n").append(result.output()).append("\n");
                        }
                        if (result.error() != null) {
                            resultMessage.append("error:\n").append(result.error()).append("\n");
                        }
                        resultMessage.append("【End Task Result】");

                        state.add(new AssistantMessage(resultMessage.toString()));

                    } catch (InterruptedException e) {
                        log.info("Task {} 执行被中断", dagTask.task().id());
                        Thread.currentThread().interrupt();
                        dagTask.setStatus(TaskStatus.FAILED);
                        results.put(dagTask.task().id(),
                                new TaskResult(dagTask.task().id(), false, null, "Task execution interrupted"));
                    } catch (Exception e) {
                        dagTask.setStatus(TaskStatus.FAILED);
                        if (compositeDisposable.isDisposed() || Thread.currentThread().isInterrupted()
                                || (e.getMessage() != null && e.getMessage().contains("interrupted"))) {
                            log.info("Task {} 执行被用户停止: {}", dagTask.task().id(), e.getMessage());
                            results.put(dagTask.task().id(),
                                    new TaskResult(dagTask.task().id(), false, null, "Task execution interrupted by user"));
                        } else {
                            log.error("Task execution error", e);
                            results.put(dagTask.task().id(),
                                    new TaskResult(dagTask.task().id(), false, null, "Task execution error: " + e.getMessage()));
                        }
                    } finally {
                        if (acquired) toolSemaphore.release();
                        latch.countDown();
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();

                compositeDisposable.add(taskDisposable);
            }

            // 2d. 等待本轮所有就绪任务完成
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("executePlan interrupted");
                break;
            }

            // 2e. 收集已终结的任务 ID
            Set<String> terminatedIds = dag.values().stream()
                    .filter(dt -> dt.status() == TaskStatus.COMPLETED || dt.status() == TaskStatus.FAILED)
                    .map(dt -> dt.task().id())
                    .collect(Collectors.toSet());

            // 2f. 级联跳过：blockedBy 含 FAILED 任务的 PENDING 任务标记 SKIPPED
            for (DagTask dt : dag.values()) {
                if (dt.status() == TaskStatus.PENDING && dt.task().blockedBy() != null) {
                    boolean hasFailedUpstream = dt.task().blockedBy().stream()
                            .anyMatch(depId -> {
                                DagTask dep = dag.get(depId);
                                return dep != null && dep.status() == TaskStatus.FAILED;
                            });
                    if (hasFailedUpstream) {
                        dt.setStatus(TaskStatus.SKIPPED);
                        terminatedIds.add(dt.task().id());
                        results.put(dt.task().id(),
                                new TaskResult(dt.task().id(), false, null, "上游任务失败，跳过执行"));
                    }
                }
            }

            // 2g. 从下游 blockedBy 中移除已终结的任务
            for (DagTask dt : dag.values()) {
                if (dt.status() == TaskStatus.PENDING) {
                    dt.removeBlockedBy(terminatedIds);
                }
            }

            // 2h. 从 DAG 中移除已终结任务
            terminatedIds.forEach(dag::remove);
        }

        return results;
    }


    /**
     * 执行单个任务（带上下文）
     * 上下文格式：【Available Results】\n[依赖结果]\n\n【Current Task】\n[任务指令]
     *
     * @param task              要执行的任务
     * @param dependencyContext 依赖上下文（只包含依赖结果）
     * @param sink              响应流
     * @param hasSentFinal      是否已发送最终结果
     * @param thinkingBuffer    思考过程缓冲
     * @return 任务执行结果
     */
    private TaskResult executeWithRetry(PlanTask task, String dependencyContext,
                                        Sinks.Many<String> sink, AtomicBoolean hasSentFinal, StringBuilder thinkingBuffer) {

        Throwable lastError = null;
        emit(sink, hasSentFinal, "⚙️ 正在执行任务 " + task.id() + " : " + task.instruction() + "\n", "thinking", thinkingBuffer);

        // 检查是否已被停止
        if (hasSentFinal.get() || compositeDisposable.isDisposed()) {
            return new TaskResult(task.id(), false, null, "任务被用户停止");
        }
        try {
            // 构建完整任务上下文（依赖 + 当前任务指令）
            String fullContext = """
                                【Available Results】
                                %s
                                
                                【Current Task】
                                %s
                    """.formatted(
                    dependencyContext,
                    task.instruction()
            );

            SimpleReactAgent agent = SimpleReactAgent.builder()
                    .chatModel(chatModel)
                    .tools(tools)
                    .maxRounds(5)
                    .systemPrompt(PlanExecutePrompts.EXECUTE)
                    .build();

            SimpleReactResult result = agent.callWithReference(null, fullContext);

            if (compositeDisposable.isDisposed()) {
                return new TaskResult(task.id(), false, null, "任务被用户停止");
            }

            // 收集搜索结果到 allReferences
            if (result.getSearchResults() != null && !result.getSearchResults().isEmpty()) {
                synchronized (allReferences) {
                    allReferences.addAll(result.getSearchResults());
                }
            }

            String answer = result.getAnswer();
            emit(sink, hasSentFinal, "执行结果: " + answer + "\n\n", "thinking", thinkingBuffer);
            return new TaskResult(task.id(), true, answer, null);
        } catch (Exception e) {
            // 检查是否是中断导致的异常
            if (compositeDisposable.isDisposed() || Thread.currentThread().isInterrupted()
                    || (e.getMessage() != null && e.getMessage().contains("interrupted"))) {
                log.info("Task {} 执行被用户停止: {}", task.id(), e.getMessage());
                return new TaskResult(task.id(), false, null, "任务被用户停止");
            }
            lastError = e;
            log.warn("Task {} failed: {}", task.id(), e.getMessage());
        }

        // 执行失败
        emit(sink, hasSentFinal, "\n❌ 任务 " + task.id() + " 执行失败: " + (lastError == null ? "unknown error" : lastError.getMessage()) + "\n\n", "thinking", thinkingBuffer);
        return new TaskResult(
                task.id(),
                false,
                null,
                lastError == null ? "unknown error" : lastError.getMessage()
        );
    }

    /**
     * 构建任务执行的依赖上下文
     * 基于 blockedBy 字段，只传递当前任务直接依赖的前置任务结果
     * 注意：此方法只返回【Available Results】部分，【Current Task】由 executeWithRetry 拼接
     *
     * @param results 所有已完成任务的结果
     * @param dag     当前 DAG（用于查找依赖任务的指令）
     * @param task    当前要执行的任务
     * @return 依赖上下文字符串
     */
    private String buildDependencyContext(Map<String, String> results, Map<String, DagTask> dag, PlanTask task) {
        StringBuilder context = new StringBuilder();

        List<String> blockedBy = task.blockedBy();
        if (blockedBy == null || blockedBy.isEmpty()) {
            return context.append("无\n").toString();
        }

        boolean hasDependencies = false;
        for (String depId : blockedBy) {
            String result = results.get(depId);
            if (result != null) {
                if (!hasDependencies) hasDependencies = true;
                DagTask depTask = dag.get(depId);
                String depInstruction = depTask != null ? depTask.task().instruction() : "";
                context.append(String.format("任务 %s (%s): %s\n\n", depId, depInstruction, result));
            }
        }

        if (!hasDependencies) {
            context.append("无\n");
        }

        return context.toString();
    }


    /**
     * 批判当前轮次的研究结果
     * 上下文：用户问题 + 研究主题 + 当前轮次的执行计划 + 当前轮次的工具结果
     *
     * @param state          整体状态
     * @param currentPlan    当前轮次的执行计划
     * @param currentResults 当前轮次的任务执行结果
     * @param sink           响应流
     * @param hasSentFinal   是否已发送最终结果
     * @param thinkingBuffer 思考过程缓冲
     * @return 批判结果
     */
    private CritiqueResult critique(OverAllState state, List<PlanTask> currentPlan,
                                    Map<String, TaskResult> currentResults,
                                    Sinks.Many<String> sink, AtomicBoolean hasSentFinal,
                                    StringBuilder thinkingBuffer) {
        BeanOutputConverter<CritiqueResult> converter = new BeanOutputConverter<>(new ParameterizedTypeReference<>() {
        });

        emit(sink, hasSentFinal, "\n🔍 正在评估当前研究结果...\n", "thinking", thinkingBuffer);

        if (hasSentFinal.get() || compositeDisposable.isDisposed()) {
            return new CritiqueResult(true, "任务已取消");
        }

        // 构建批判的用户消息（只包含当前轮次的上下文）
        StringBuilder userMessage = new StringBuilder();
        userMessage.append("【用户原始问题】\n");
        userMessage.append(state.getQuestion());

        userMessage.append("\n\n【研究主题】\n");
        userMessage.append(state.getRefinedResearchTopic() != null ?
                state.getRefinedResearchTopic() : "未生成研究主题");

        userMessage.append("\n\n【当前轮次的执行计划】\n");
        if (currentPlan != null && !currentPlan.isEmpty()) {
            for (PlanTask task : currentPlan) {
                userMessage.append(String.format("- %s\n", task.instruction()));
            }
        } else {
            userMessage.append("无\n");
        }

        userMessage.append("\n\n【当前轮次的工具结果】\n");
        if (currentResults != null && !currentResults.isEmpty()) {
            for (Map.Entry<String, TaskResult> entry : currentResults.entrySet()) {
                TaskResult result = entry.getValue();
                if (result != null && result.success() && result.output() != null) {
                    userMessage.append(String.format("任务 %s: %s\n\n",
                            entry.getKey(), result.output()));
                } else if (result != null && !result.success() && result.error() != null) {
                    userMessage.append(String.format("任务 %s: 执行失败 - %s\n\n",
                            entry.getKey(), result.error()));
                }
            }
        } else {
            userMessage.append("无\n");
        }

        String prom = PlanExecutePrompts.CRITIQUE + "\n" + converter.getFormat();
        Prompt prompt = new Prompt(List.of(
                new SystemMessage(PlanExecutePrompts.getCurrentTime()),
                new SystemMessage(prom),
                new UserMessage(userMessage.toString())
        ));

        String raw = chatClient.prompt(prompt).call().content();

        CritiqueResult result = converter.convert(raw);

        if (result.passed()) {
            emit(sink, hasSentFinal, "\n✅ 研究结果评估通过，准备生成最终报告\n", "thinking", thinkingBuffer);
        } else {
            emit(sink, hasSentFinal, "\n⚠️ 研究结果评估未通过，原因分析：" + result.feedback() + "\n", "thinking", thinkingBuffer);
        }

        return result;
    }

    private void compressIfNeeded(OverAllState state, Sinks.Many<String> sink, AtomicBoolean hasSentFinal, StringBuilder thinkingBuffer) {
        if (state.currentChars() < contextCharLimit) {
            return;
        }

        log.warn("===== Context too large, compressing ,size is {} =====", state.currentChars());

        emit(sink, hasSentFinal, "📦 上下文过长，正在压缩...\n", "thinking", thinkingBuffer);

        if (hasSentFinal.get() || compositeDisposable.isDisposed()) {
            return;
        }

        Prompt prompt = new Prompt(List.of(
                new SystemMessage(PlanExecutePrompts.getCurrentTime()),
                new SystemMessage("""
                        ##最大压缩限制（必须遵守）
                        -你输出的最终内容【总字符数（包含所有标签、空格、换行）】
                        不得超过：%s
                                - 这是硬性上限，不是建议
                                - 如超过该限制，视为压缩失败

                        """.formatted(contextCharLimit) + PlanExecutePrompts.COMPRESS),

                new UserMessage(state.renderFullContext())
        ));

        String snapshot = chatModel.call(prompt)
                .getResult()
                .getOutput()
                .getText();

        state.clearMessages();
        state.add(new SystemMessage("【Compressed Agent State】\n" + snapshot));
        log.warn("===== Context compress has completed, size is {} =====", state.currentChars());

        emit(sink, hasSentFinal, "✅ 上下文压缩完成\n", "thinking", thinkingBuffer);
    }


    private void summarizeStream(OverAllState state,
                                 Sinks.Many<String> sink,
                                 AtomicBoolean finished,
                                 StringBuilder finalAnswerBuffer,
                                 StringBuilder thinkingBuffer) {

        emit(sink, finished, "\n📝 正在生成最终研究报告...\n\n", "thinking", thinkingBuffer);

        // 提取工具执行结果，排除中间过程
        String toolResults = state.extractToolResults();

        // 构建 prompt，注入用户画像
        List<Message> promptMessages = new ArrayList<>();
        promptMessages.add(new SystemMessage(PlanExecutePrompts.getCurrentTime()));
        promptMessages.add(new SystemMessage(PlanExecutePrompts.SUMMARIZE));
        String userMemoryContext = loadUserMemoryContext();
        if (userMemoryContext != null) {
            promptMessages.add(new SystemMessage(userMemoryContext));
        }
        promptMessages.add(new UserMessage("""
                【用户原始问题】
                %s

                【研究主题】
                %s

                【工具检索结果】
                %s
                """.formatted(
                state.getQuestion(),
                state.getRefinedResearchTopic() != null ? state.getRefinedResearchTopic() : "未生成研究主题",
                toolResults.isEmpty() ? "（未检索到相关结果）" : toolResults
        )));

        Prompt prompt = new Prompt(promptMessages);

        Disposable disposable = chatClient.prompt()
                .messages(prompt.getInstructions())
                .stream()
                .chatResponse()
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(chunk -> {

                    if (finished.get() || compositeDisposable.isDisposed()) {
                        return;
                    }

                    if (chunk == null
                            || chunk.getResult() == null
                            || chunk.getResult().getOutput() == null) {
                        return;
                    }

                    String text = chunk.getResult().getOutput().getText();
                    if (text == null) {
                        return;
                    }

                    finalAnswerBuffer.append(text);

                    emit(sink, finished, text, "text");
                })
                .doOnComplete(() -> {
                    // 在 text 输出后，输出参考来源
                    if (!allReferences.isEmpty()) {
                        sink.tryEmitNext(createReferenceResponse(JSON.toJSONString(allReferences)));
                    }

                    complete(sink, finished);
                })
                .doOnError(e -> error(sink, finished, e))
                .subscribe();

        // 将summarizeStream的disposable添加到composite
        compositeDisposable.add(disposable);
    }

    private String renderToolDescriptions() {
        if (tools == null || tools.isEmpty()) {
            return "（当前无可用工具）";
        }

        StringBuilder sb = new StringBuilder();
        for (ToolCallback tool : tools) {
            sb.append("- ")
                    .append(tool.getToolDefinition().name())
                    .append(": ")
                    .append(tool.getToolDefinition().description())
                    .append("\n");
        }
        return sb.toString();
    }

    /**
     * DAG 调度器使用的 mutable 包装器
     */
    private static class DagTask {
        private final PlanTask task;
        private volatile TaskStatus status;
        private final List<String> blockedBy;

        DagTask(PlanTask task) {
            this.task = task;
            this.status = TaskStatus.PENDING;
            this.blockedBy = new ArrayList<>(
                    task.blockedBy() != null ? task.blockedBy() : List.of());
        }

        PlanTask task() { return task; }
        TaskStatus status() { return status; }
        void setStatus(TaskStatus s) { this.status = s; }

        boolean isReady() {
            return status == TaskStatus.PENDING && blockedBy.isEmpty();
        }

        void removeBlockedBy(Set<String> ids) {
            blockedBy.removeAll(ids);
        }
    }

    /**
     * 检测任务依赖环，返回环中节点描述（无环返回 null）
     * Kahn 算法：拓扑排序后，未处理的节点即为环中节点
     */
    private String detectCycleDetail(List<PlanTask> plan) {
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> graph = new HashMap<>();

        for (PlanTask t : plan) {
            if (t.id() == null) continue;
            inDegree.putIfAbsent(t.id(), 0);
            graph.putIfAbsent(t.id(), new ArrayList<>());
            if (t.blockedBy() != null) {
                for (String dep : t.blockedBy()) {
                    inDegree.merge(t.id(), 1, Integer::sum);
                    graph.computeIfAbsent(dep, k -> new ArrayList<>()).add(t.id());
                }
            }
        }

        Queue<String> queue = new LinkedList<>();
        for (var e : inDegree.entrySet()) {
            if (e.getValue() == 0) queue.add(e.getKey());
        }

        int processed = 0;
        while (!queue.isEmpty()) {
            String id = queue.poll();
            processed++;
            for (String next : graph.getOrDefault(id, List.of())) {
                if (inDegree.merge(next, -1, Integer::sum) == 0) {
                    queue.add(next);
                }
            }
        }

        if (processed == inDegree.size()) return null;

        Set<String> cycleNodes = inDegree.keySet().stream()
                .filter(id -> inDegree.get(id) > 0)
                .collect(Collectors.toSet());
        return String.join(" ↔ ", cycleNodes);
    }
}
