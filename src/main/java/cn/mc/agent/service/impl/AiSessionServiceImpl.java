package cn.mc.agent.service.impl;

import cn.mc.agent.entity.AiSession;
import cn.mc.agent.entity.request.SaveQuestionRequest;
import cn.mc.agent.entity.request.UpdateAnswerRequest;
import cn.mc.agent.mapper.AiSessionMapper;
import cn.mc.agent.prompts.PlanExecutePrompts;
import cn.mc.agent.service.AiSessionService;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
public class AiSessionServiceImpl extends ServiceImpl<AiSessionMapper, AiSession> implements AiSessionService {

    private static final int SUMMARY_MIN_ANSWER_LENGTH = 2000;

    private final ChatModel chatModel;

    public AiSessionServiceImpl(ChatModel chatModel) {
        this.chatModel = chatModel;
    }

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

    @Override
    public void generateSummary(Long sessionId) {
        AiSession session = this.getById(sessionId);
        if (session == null || session.getAnswer() == null) {
            return;
        }
        if (session.getAnswer().length() < SUMMARY_MIN_ANSWER_LENGTH) {
            return;
        }
        if (session.getSummary() != null && !session.getSummary().isBlank()) {
            return;
        }

        try {
            String summary = ChatClient.builder(chatModel).build()
                    .prompt()
                    .system(PlanExecutePrompts.SESSION_SUMMARY)
                    .user(session.getAnswer())
                    .call()
                    .content();

            if (summary != null && !summary.isBlank()) {
                session.setSummary(summary);
                this.updateById(session);
                log.info("摘要生成成功: sessionId={}, 摘要长度={}", sessionId, summary.length());
            }
        } catch (Exception e) {
            log.error("摘要生成失败: sessionId={}", sessionId, e);
        }
    }

}