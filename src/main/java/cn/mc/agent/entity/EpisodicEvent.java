package cn.mc.agent.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@TableName("episodic_memory")
public class EpisodicEvent {

    @TableId(value = "id", type = IdType.ASSIGN_ID)
    private Long id;

    @TableField("session_id")
    private String sessionId;

    @TableField("conversation_id")
    private String conversationId;

    @TableField("event_type")
    private String eventType;

    @TableField("content")
    private String content;

    @TableField("topic")
    private String topic;

    @TableField("create_time")
    private LocalDateTime createTime;

    public enum EventType {
        TOPIC, FINDING, DIMENSION, FAILURE
    }
}
