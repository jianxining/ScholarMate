package cn.mc.agent.service;

import cn.mc.agent.entity.SemanticMemory;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

public interface SemanticMemoryService extends IService<SemanticMemory> {

    SemanticMemory findByUserId(String userId);

    void updateMemory(String userId, List<JSONObject> updates);
}
