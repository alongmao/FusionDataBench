package runner;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import type.Relationship;

import java.util.Map;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/15 21:45
 * @Version 1.0
 */
@Data
public class RelationshipFilter {

    private String type;

    private Map<String, Object> properties;

    public Boolean match(Relationship relationship) {
        if (StringUtils.isEmpty(this.type)) {
            return true;
        } else if (null == this.properties) {
            return false;
        }
        return this.type.equals(relationship.getRelationshipType()) &&
                relationship.keys().containsAll(this.properties.keySet()) &&
                relationship.keys().stream().allMatch(e ->
                        relationship.property(e).equals(this.properties.get(e))
                );
    }
}
