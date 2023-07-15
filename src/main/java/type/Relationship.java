package type;


import lombok.Data;

import java.util.Map;
import java.util.Optional;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/15 21:14
 * @Version 1.0
 */
@Data
public class Relationship extends HasProperty {

    private Integer id;

    private Integer startId;

    private Integer endId;

    private String relationshipType;

    private Map<String, Object> properties;


    @Override
    public Object property(String key) {
        return properties.get(key);
    }
}
