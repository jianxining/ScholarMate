/**
 * 常量定义文件
 * 存放应用中的固定常量
 */

// 智能体列表（秘塔风格标签）
const AGENTS = [
    { id: 'chat', name: '对话助手', icon: 'fas fa-comment-dots' },
    { id: 'file', name: '文件问答', icon: 'fas fa-file-upload' },
    { id: 'deep', name: '深度研究', icon: 'fas fa-flask' }
];

// 支持的文件类型
const SUPPORTED_FILE_TYPES = {
    mime: [
        'application/pdf',
        'application/msword',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'text/plain',
        'image/png',
        'image/jpeg',
        'image/jpg'
    ],
    extensions: ['pdf', 'doc', 'docx', 'txt', 'png', 'jpg', 'jpeg']
};

// 流式消息类型
const STREAM_TYPES = {
    TEXT: 'text',
    THINKING: 'thinking',
    REFERENCE: 'reference',
    RECOMMEND: 'recommend',
    COMPLETE: 'complete',
    DONE: '[DONE]'
};

// 研究深度
const RESEARCH_DEPTHS = {
    CONCISE: 'concise',
    DEEP: 'deep',
    RESEARCH: 'research'
};

// 导出常量（用于非模块化环境）
window.APP_CONSTANTS = {
    AGENTS,
    SUPPORTED_FILE_TYPES,
    STREAM_TYPES,
    RESEARCH_DEPTHS
};
