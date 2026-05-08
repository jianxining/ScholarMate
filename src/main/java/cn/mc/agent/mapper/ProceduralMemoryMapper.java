package cn.mc.agent.mapper;

import cn.mc.agent.entity.ProceduralMemory;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface ProceduralMemoryMapper extends BaseMapper<ProceduralMemory> {

    @Select("SELECT * FROM procedural_memory WHERE user_id = #{userId} LIMIT 1")
    ProceduralMemory findByUserId(@Param("userId") String userId);
}
