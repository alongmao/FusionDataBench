package type;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/25 22:12
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@ToString
public class PathTriple {

    private Node startNode;

    private Node endNode;

    private Relationship storeRelationship;

    private boolean reverse;
}
