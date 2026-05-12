package cn.mc.agent.mapper;

import cn.mc.agent.entity.EpisodicEvent;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface EpisodicEventMapper extends BaseMapper<EpisodicEvent> {

    @Select("SELECT * FROM episodic_memory WHERE session_id = #{sessionId} ORDER BY create_time ASC")
    List<EpisodicEvent> findBySessionId(@Param("sessionId") String sessionId);

    @Select("SELECT * FROM episodic_memory WHERE session_id = #{sessionId} AND topic LIKE CONCAT('%', #{topicKeyword}, '%') ORDER BY create_time DESC")
    List<EpisodicEvent> findBySessionIdAndTopic(@Param("sessionId") String sessionId, @Param("topicKeyword") String topicKeyword);

    @Select("SELECT * FROM episodic_memory WHERE session_id = #{sessionId} ORDER BY create_time DESC LIMIT #{limit}")
    List<EpisodicEvent> findRecentBySessionId(@Param("sessionId") String sessionId, @Param("limit") int limit);
}
