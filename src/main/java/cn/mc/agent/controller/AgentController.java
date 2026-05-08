package cn.mc.agent.controller;


import cn.mc.agent.agent.deepresearch.PlanExecuteAgent;
import cn.mc.agent.agent.file.FileReactAgent;
import cn.mc.agent.agent.pptx.PPTBuilderAgent;
import cn.mc.agent.agent.webSearch.WebSearchReactAgent;
import cn.mc.agent.service.AgentTaskManager;
import cn.mc.agent.service.AiSessionService;
import cn.mc.agent.service.EpisodicMemoryService;
import cn.mc.agent.tool.FileContentService;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.swagger.v3.oas.annotations.Operation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.mcp.SyncMcpToolCallbackProvider;
import org.springframework.ai.support.ToolCallbacks;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author tmengchun
 * @description
 * @create 2026/4/24 11:12
 */

@RestController
@RequestMapping("/agent")
@Slf4j

public class AgentController implements InitializingBean {

    @Autowired
    private ChatModel chatModel;

    @Autowired
    private AiSessionService sessionService;

    @Autowired
    private FileContentService fileContentService;

    @Autowired
    private AgentTaskManager taskManager;

    @Autowired
    private EpisodicMemoryService episodicMemoryService;

    @Value("${tavily.api-key}")
    private String tavilyApiKey;

    /**
     * Tavily MCP URL
     */
    @Value("${tavily.mcp-url}")
    private String tavilyMcpUrl;


    private ToolCallback[] webSearchToolCallbacks;


    @GetMapping(value = "/chat/stream", produces = "text/event-stream;charset=UTF-8")
    public Flux<String> webSearchStream(@RequestParam(required = true) String query,
                                        @RequestParam(required = true) String conversationId) {
        log.info("收到Web搜索请求: query={}, conversationId={}", query, conversationId);
        // 参数校验
        if (query == null || query.trim().isEmpty()) {
            return Flux.error(new IllegalArgumentException("查询参数不能为空"));
        }
        // 初始化搜索Agent
        WebSearchReactAgent webSearchReactAgent = initWebSearchAgent();
        ChatMemory persistentMemory = webSearchReactAgent.createPersistentChatMemory(conversationId, 30);
        webSearchReactAgent.setChatMemory(persistentMemory);
        return webSearchReactAgent.stream(conversationId, query);
    }

    @GetMapping(value = "/file/stream", produces = "text/event-stream;charset=UTF-8")
    public Flux<String> fileStream(@RequestParam(required = true) String query,
                                   @RequestParam(required = true) String conversationId,
                                   @RequestParam(required = true) String fileId) {
        log.info("收到文件问答请求: query={}, conversationId={}, fileId={}", query, conversationId, fileId);

        if (query == null || query.trim().isEmpty()) {
            log.warn("查询参数为空或无效");
            return Flux.error(new IllegalArgumentException("查询参数不能为空"));
        }

        if (fileId == null || fileId.trim().isEmpty()) {
            log.warn("文件ID参数为空");
            return Flux.error(new IllegalArgumentException("文件ID不能为空"));
        }

        try {
            FileReactAgent fileReactAgent = initFileReactAgent();
            // 使用持久化记忆加载历史记录
            ChatMemory persistentMemory = fileReactAgent.createPersistentChatMemory(conversationId, 30);
            fileReactAgent.setChatMemory(persistentMemory);
            return fileReactAgent.stream(conversationId, query, fileId);
        } catch (Exception e) {
            log.error("处理文件问答请求时发生错误: ", e);
            return Flux.error(e);
        }
    }

    @GetMapping(value = "/pptx/stream", produces = "text/event-stream;charset=UTF-8")
    @Operation(summary = "PPT 生成", description = "接收用户需求并返回流式响应，基于模板驱动生成PPT")
    public Flux<String> pptxStream(@RequestParam(required = true) String query,
                                   @RequestParam(required = true) String conversationId) {
        log.info("收到PPT Builder请求: query={}, conversationId={}", query, conversationId);

        if (query == null || query.trim().isEmpty()) {
            log.warn("查询参数为空或无效");
            return Flux.error(new IllegalArgumentException("查询参数不能为空"));
        }

        try {
            PPTBuilderAgent pptBuilderAgent = initPPTBuilderAgent();
            // 使用持久化记忆加载历史记录
            ChatMemory persistentMemory = pptBuilderAgent.createPersistentChatMemory(conversationId, 30);
            pptBuilderAgent.setChatMemory(persistentMemory);
            return pptBuilderAgent.execute(conversationId, query);
        } catch (Exception e) {
            log.error("处理PPT Builder请求时发生错误: ", e);
            return Flux.error(e);
        }
    }

    @GetMapping(value = "/deep/stream", produces = "text/event-stream;charset=UTF-8")
    @Operation(summary = "深度研究", description = "接收用户查询并返回流式响应，使用计划-执行模式进行深度研究")
    public Flux<String> deepStream(@RequestParam(required = true) String query,
                                   @RequestParam(required = true) String conversationId) {
        log.info("收到深度研究请求: query={}, conversationId={}", query, conversationId);

        if (query == null || query.trim().isEmpty()) {
            log.warn("查询参数为空或无效");
            return Flux.error(new IllegalArgumentException("查询参数不能为空"));
        }

        try {
            PlanExecuteAgent planExecuteAgent = initPlanExecuteAgent();
            // 使用持久化记忆加载历史记录
            ChatMemory persistentMemory = planExecuteAgent.createPersistentChatMemory(conversationId, 30);
            planExecuteAgent.setChatMemory(persistentMemory);
            return planExecuteAgent.stream(conversationId, query);
        } catch (Exception e) {
            log.error("处理深度研究请求时发生错误: ", e);
            return Flux.error(e);
        }
    }



    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("开始初始化工具tool callbacks...");
        initWebSearchToolCallbacks();
        log.info("工具tool callbacks初始化完成！");
    }

    /**
     * 初始化网页搜索工具回调
     */
    private void initWebSearchToolCallbacks() throws Exception {
        log.info("初始化网页搜索工具回调...");

        // tavily 搜索引擎
        String authorizationHeader = "Bearer " + tavilyApiKey;

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .header("Authorization", authorizationHeader);

        HttpClientStreamableHttpTransport tavTransport = HttpClientStreamableHttpTransport.builder(tavilyMcpUrl)
                .requestBuilder(requestBuilder).build();
        McpSyncClient tavilyMcp = McpClient.sync(tavTransport)
                .requestTimeout(Duration.ofSeconds(300))
                .build();
        tavilyMcp.initialize();

        List<McpSyncClient> mcpClients = List.of(tavilyMcp);
        SyncMcpToolCallbackProvider provider = SyncMcpToolCallbackProvider.builder().mcpClients(mcpClients).build();

        webSearchToolCallbacks = provider.getToolCallbacks();
        log.info("网页搜索工具回调初始化完成，工具数量: {}", webSearchToolCallbacks.length);
    }


    @GetMapping("/stop")
    public Map<String, Object> stopAgent(@RequestParam String conversationId) {
        log.info("收到停止请求: conversationId={}", conversationId);
        boolean success = taskManager.stopTask(conversationId);
        Map<String, Object> result = new HashMap<>();
        if (success) {
            result.put("success", true);
            result.put("message", "已停止执行");
        } else {
            result.put("success", false);
            result.put("message", "没有找到正在执行的任务或已停止");
        }
        return result;
    }

    /**
     * 初始化网页搜索 Agent
     */
    private WebSearchReactAgent initWebSearchAgent() {
        log.info("初始化网页搜索 Agent...");

        return WebSearchReactAgent.builder()
                .name("web react agent")
                .chatModel(chatModel)
                .tools(webSearchToolCallbacks)
                .sessionService(sessionService)
                .taskManager(taskManager)
                .maxRounds(5)
                .build();
    }


    /**
     * 初始化文件问答 Agent
     */
    private FileReactAgent initFileReactAgent() {
        log.info("初始化文件问答 Agent...");

        List<ToolCallback> allTools = Arrays.asList(ToolCallbacks.from(fileContentService));

        return FileReactAgent.builder()
                .name("file react")
                .chatModel(chatModel)
                .tools(allTools)
                .sessionService(sessionService)
                .taskManager(taskManager)
                .build();
    }

    /**
     * 初始化PPT Builder Agent
     */
    private PPTBuilderAgent initPPTBuilderAgent() {
        log.info("初始化PPT Builder Agent...");

        return new PPTBuilderAgent(
                chatModel,
                Arrays.asList(webSearchToolCallbacks),
                sessionService,
                taskManager);
    }

    /**
     * 初始化 PlanExecute Agent
     */
    private PlanExecuteAgent initPlanExecuteAgent() {
        log.info("初始化 PlanExecute Agent...");

        return PlanExecuteAgent.builder()
                .chatModel(chatModel)
                .tools(webSearchToolCallbacks)
                .sessionService(sessionService)
                .taskManager(taskManager)
                .episodicMemoryService(episodicMemoryService)
                .maxRounds(3)
                .build();
    }


}
