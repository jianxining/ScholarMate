package cn.mc.agent.service.impl;

import cn.mc.agent.entity.ProceduralMemory;
import cn.mc.agent.mapper.ProceduralMemoryMapper;
import cn.mc.agent.service.ProceduralMemoryService;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
public class ProceduralMemoryServiceImpl extends ServiceImpl<ProceduralMemoryMapper, ProceduralMemory> implements ProceduralMemoryService {

    private static final int CONFIDENCE_THRESHOLD = 60;

    @Override
    public ProceduralMemory findByUserId(String userId) {
        return baseMapper.findByUserId(userId);
    }

    @Override
    public void updateMemory(String userId, List<JSONObject> updates) {
        if (updates == null || updates.isEmpty()) {
            return;
        }

        ProceduralMemory memory = findByUserId(userId);
        boolean isNew = memory == null;
        if (isNew) {
            memory = new ProceduralMemory();
            memory.setUserId(userId);
            memory.setCreateTime(LocalDateTime.now());
            memory.setConfidence(0);
        }

        int maxConfidence = memory.getConfidence() != null ? memory.getConfidence() : 0;

        for (JSONObject update : updates) {
            int confidence = update.getIntValue("confidence", 0);
            if (confidence < CONFIDENCE_THRESHOLD) {
                log.debug("置信度过低，跳过: category={}, confidence={}", update.getString("category"), confidence);
                continue;
            }

            String category = update.getString("category");
            String value = update.getString("value");
            if (category == null || value == null) continue;

            switch (category) {
                case "interaction_style" -> memory.setInteractionStyle(value);
                case "detail_tolerance" -> memory.setDetailTolerance(value);
                case "workflow_pattern" -> memory.setWorkflowPattern(value);
                default -> log.debug("未知的程序记忆类别: {}", category);
            }

            if (confidence > maxConfidence) {
                maxConfidence = confidence;
            }
        }

        memory.setConfidence(maxConfidence);
        memory.setUpdateTime(LocalDateTime.now());

        if (isNew) {
            this.save(memory);
        } else {
            this.updateById(memory);
        }
        log.info("程序记忆更新成功: userId={}, isNew={}", userId, isNew);
    }
}
