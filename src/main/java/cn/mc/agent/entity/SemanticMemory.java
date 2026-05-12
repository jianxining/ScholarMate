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
@TableName("semantic_memory")
public class SemanticMemory {

    @TableId(value = "id", type = IdType.ASSIGN_ID)
    private Long id;

    @TableField("user_id")
    private String userId;

    @TableField("industry")
    private String industry;

    @TableField("role")
    private String role;

    @TableField("research_purpose")
    private String researchPurpose;

    @TableField("report_preference")
    private String reportPreference;

    @TableField("language")
    private String language;

    @TableField("domain_expertise")
    private String domainExpertise;

    @TableField("confidence")
    private Integer confidence;

    @TableField("create_time")
    private LocalDateTime createTime;

    @TableField("update_time")
    private LocalDateTime updateTime;
}
