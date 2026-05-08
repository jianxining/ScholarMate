package cn.mc.agent.service;

import cn.mc.agent.entity.ProceduralMemory;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

public interface ProceduralMemoryService extends IService<ProceduralMemory> {

    ProceduralMemory findByUserId(String userId);

    void updateMemory(String userId, List<JSONObject> updates);
}
