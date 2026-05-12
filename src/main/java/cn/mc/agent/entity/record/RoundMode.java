package cn.mc.agent.entity.record;

import lombok.Getter;

@Getter
public enum RoundMode {
    /**
     * 未知模式
     */
    UNKNOWN("未知"),
    /**
     * 最终答案模式
     */
    FINAL_ANSWER("最终答案"),
    /**
     * 工具调用模式
     */
    TOOL_CALL("工具调用");

    private final String desc;

    RoundMode(String desc) {
        this.desc = desc;
    }
}
