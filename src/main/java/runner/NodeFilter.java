package runner;

import lombok.Data;
import type.Node;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/15 21:24
 * @Version 1.0
 */
@Data
public class NodeFilter {

    private List<String> labels;
    private Map<String, Object> properties;

    public Boolean match(Node node) {
        Boolean b1 = node.getLabels().containsAll(labels);
        Boolean b2 = node.keys().containsAll(properties.keySet());
        Boolean b3 = properties.keySet().stream().allMatch(e -> {
            if (node.keys().contains(e)) {
                return properties.get(e).toString().equals(node.property(e).toString());
            }
            return false;
        });
        return b1 && b2 && b3;
    }
}
