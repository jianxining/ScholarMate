CREATE TABLE semantic_memory (
    id BIGINT PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    industry VARCHAR(64) COMMENT '所在行业',
    role VARCHAR(64) COMMENT '职业角色',
    research_purpose VARCHAR(32) COMMENT 'academic/business/personal',
    report_preference VARCHAR(32) COMMENT 'DETAILED/CONCISE/DATA_DRIVEN',
    language VARCHAR(16) COMMENT 'zh/en',
    domain_expertise JSON COMMENT '领域知识水平 {"领域":"LEVEL"}',
    confidence INT DEFAULT 50 COMMENT '整体置信度 0-100',
    create_time DATETIME NOT NULL,
    update_time DATETIME NOT NULL,
    UNIQUE KEY uk_user_id (user_id)
) COMMENT '用户语义记忆（长期稳定画像）';
