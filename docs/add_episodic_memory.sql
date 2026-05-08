CREATE TABLE episodic_memory (
    id BIGINT PRIMARY KEY,
    session_id VARCHAR(64) NOT NULL COMMENT '会话ID',
    conversation_id VARCHAR(64) COMMENT '对话ID',
    event_type VARCHAR(32) NOT NULL COMMENT '事件类型：TOPIC/FINDING/DIMENSION/FAILURE',
    content TEXT NOT NULL COMMENT '事件内容描述',
    topic VARCHAR(128) COMMENT '主题关键词，用于检索',
    create_time DATETIME NOT NULL,
    INDEX idx_session_id (session_id),
    INDEX idx_topic (topic)
) COMMENT '结构化事件记忆表';
