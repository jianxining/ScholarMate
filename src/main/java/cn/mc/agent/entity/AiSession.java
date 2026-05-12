package cn.mc.agent.entity;


import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * @author tmengchun
 * @description
 * @create 2026/4/24 12:33
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AiSession {

    /**
     * 主键ID
     */
    @TableId(value = "id", type = IdType.ASSIGN_ID)
    private Long id;

    /**
     * 会话ID
     */
    @TableField("session_id")
    private String sessionId;

    /**
     * 智能体类型（react/file/ppt）
     */
    @TableField("agent_type")
    private String agentType;

    /**
     * 用户问题
     */
    @TableField("question")
    private String question;

    /**
     * AI回复
     */
    @TableField("answer")
    private String answer;

    /**
     * 涉及的执行工具名称（逗号分隔）
     */
    @TableField("tools")
    private String tools;

    /**
     * 参考链接JSON
     */
    @TableField("reference")
    private String reference;

    /**
     * 首次响应时间（毫秒）
     */
    @TableField("first_response_time")
    private Long firstResponseTime;

    /**
     * 整体回复时间（毫秒）
     */
    @TableField("total_response_time")
    private Long totalResponseTime;

    /**
     * 创建时间
     */
    @TableField("create_time")
    private LocalDateTime createTime;

    /**
     * 更新时间
     */
    @TableField("update_time")
    private LocalDateTime updateTime;

    /**
     * 思考过程
     */
    @TableField("thinking")
    private String thinking;

    /**
     * 关联文件ID
     */
    @TableField("fileid")
    private String fileid;

    /**
     * 推荐问题JSON
     */
    @TableField("recommend")
    private String recommend;

    /**
     * 执行后摘要（用于跨轮次中期记忆）
     */
    @TableField("summary")
    private String summary;
}
