package type;

import lombok.Data;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/15 21:05
 * @Version 1.0
 */
@Data
public abstract class HasProperty {

    private Set<String> keyList;

    private Set<String> intelKeyList;

    public Set<String> keys() {
        return this.keyList;
    }

    public abstract Object property(String key);

}
