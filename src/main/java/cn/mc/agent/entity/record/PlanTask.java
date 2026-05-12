package cn.mc.agent.entity.record;

import java.util.List;

/**
 * 计划任务记录（DAG 模型）
 */
public record PlanTask(
        String id,
        String instruction,
        List<String> blockedBy
) {
}
