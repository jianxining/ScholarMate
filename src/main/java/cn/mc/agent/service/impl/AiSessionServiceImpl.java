package cn.mc.agent.service.impl;

import cn.mc.agent.entity.AiSession;
import cn.mc.agent.entity.request.SaveQuestionRequest;
import cn.mc.agent.entity.request.UpdateAnswerRequest;
import cn.mc.agent.mapper.AiSessionMapper;
import cn.mc.agent.service.AiSessionService;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class AiSessionServiceImpl extends ServiceImpl<AiSessionMapper, AiSession> implements AiSessionService {

    @Override
    public List<AiSession> findRecentBySessionId(String sessionId, int maxRecords) {
        LambdaQueryWrapper<AiSession> queryWrapper = new LambdaQueryWrapper<AiSession>()
                .eq(AiSession::getSessionId, sessionId)
                .orderByDesc(AiSession::getCreateTime)
                .last("LIMIT " + maxRecords);

        return this.list(queryWrapper);
    }

    @Override
    public AiSession saveQuestion(SaveQuestionRequest request) {
        AiSession aiSession = new AiSession();
        aiSession.setSessionId(request.getSessionId());
        aiSession.setQuestion(request.getQuestion());
        aiSession.setFileid(request.getFileid());
        aiSession.setTools(request.getTools());
        aiSession.setFirstResponseTime(request.getFirstResponseTime());
        aiSession.setCreateTime(LocalDateTime.now());
        aiSession.setUpdateTime(LocalDateTime.now());

        this.save(aiSession);
        return aiSession;
    }

    @Override
    public boolean updateAnswer(UpdateAnswerRequest request) {
        AiSession session = this.getById(request.getId());
        if (session != null) {
            session.setAnswer(request.getAnswer());
            session.setUpdateTime(LocalDateTime.now());
            if (request.getThinking() != null) {
                session.setThinking(request.getThinking());
            }
            if (request.getTools() != null) {
                session.setTools(request.getTools());
            }
            if (request.getReference() != null) {
                session.setReference(request.getReference());
            }
            if (request.getFirstResponseTime() != null) {
                session.setFirstResponseTime(request.getFirstResponseTime());
            }
            if (request.getTotalResponseTime() != null) {
                session.setTotalResponseTime(request.getTotalResponseTime());
            }
            if(request.getRecommend() != null){
                session.setRecommend(request.getRecommend());
            }
            return this.updateById(session);
        }
        return false;
    }

}