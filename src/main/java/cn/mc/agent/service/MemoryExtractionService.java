package cn.mc.agent.service;

public interface MemoryExtractionService {

    /**
     * 统一记忆提取入口：一次 LLM 调用，提取三类记忆（episodic + semantic + procedural）
     * @param aiSessionId ai_session 表的主键 ID
     */
    void extractAll(Long aiSessionId);
}
