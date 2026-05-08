CREATE TABLE procedural_memory (
    id BIGINT PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    interaction_style VARCHAR(32) COMMENT 'DEEP_DIVE/ONE_SHOT',
    detail_tolerance VARCHAR(32) COMMENT 'HIGH/LOW',
    workflow_pattern VARCHAR(64) COMMENT 'brainstorm_first/direct_report',
    confidence INT DEFAULT 50 COMMENT '整体置信度 0-100',
    create_time DATETIME NOT NULL,
    update_time DATETIME NOT NULL,
    UNIQUE KEY uk_user_id (user_id)
) COMMENT '用户程序记忆（行为模式）';
