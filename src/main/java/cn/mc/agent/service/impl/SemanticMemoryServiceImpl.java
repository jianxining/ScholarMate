package cn.mc.agent.service.impl;

import cn.mc.agent.entity.SemanticMemory;
import cn.mc.agent.mapper.SemanticMemoryMapper;
import cn.mc.agent.service.SemanticMemoryService;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
public class SemanticMemoryServiceImpl extends ServiceImpl<SemanticMemoryMapper, SemanticMemory> implements SemanticMemoryService {

    private static final int CONFIDENCE_THRESHOLD = 60;

    @Override
    public SemanticMemory findByUserId(String userId) {
        return baseMapper.findByUserId(userId);
    }

    @Override
    public void updateMemory(String userId, List<JSONObject> updates) {
        if (updates == null || updates.isEmpty()) {
            return;
        }

        SemanticMemory memory = findByUserId(userId);
        boolean isNew = memory == null;
        if (isNew) {
            memory = new SemanticMemory();
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
            if (category == null) continue;

            if ("domain_expertise".equals(category)) {
                JSONObject domains = update.getJSONObject("value");
                if (domains != null) {
                    JSONObject existing = memory.getDomainExpertise() != null
                            ? JSON.parseObject(memory.getDomainExpertise()) : new JSONObject();
                    existing.putAll(domains);
                    memory.setDomainExpertise(existing.toJSONString());
                }
            } else {
                String value = update.getString("value");
                if (value == null) continue;
                switch (category) {
                    case "industry" -> memory.setIndustry(value);
                    case "role" -> memory.setRole(value);
                    case "research_purpose" -> memory.setResearchPurpose(value);
                    case "report_preference" -> memory.setReportPreference(value);
                    case "language" -> memory.setLanguage(value);
                    default -> log.debug("未知的语义记忆类别: {}", category);
                }
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
        log.info("语义记忆更新成功: userId={}, isNew={}", userId, isNew);
    }
}
