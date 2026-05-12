package cn.mc.agent.mapper;

import cn.mc.agent.entity.SemanticMemory;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface SemanticMemoryMapper extends BaseMapper<SemanticMemory> {

    @Select("SELECT * FROM semantic_memory WHERE user_id = #{userId} LIMIT 1")
    SemanticMemory findByUserId(@Param("userId") String userId);
}
