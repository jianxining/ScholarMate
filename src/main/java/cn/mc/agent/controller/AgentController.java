package cn.mc.agent.controller;


import cn.mc.agent.agent.webSearch.WebSearchReactAgent;
import cn.mc.agent.service.AgentTaskManager;
import cn.mc.agent.service.AiSessionService;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.mcp.SyncMcpToolCallbackProvider;
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
import java.util.List;

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
    private AgentTaskManager taskManager;

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
}
