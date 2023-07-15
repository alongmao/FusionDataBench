package type;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/15 20:57
 * @Version 1.0
 */
@Data
public  class Node extends HasProperty{

    private Integer id;

    private List<String> labels;

    private Map<String,Object> properties;

    @Override
    public Object property(String key) {
        return properties.get(key);
    }
}
