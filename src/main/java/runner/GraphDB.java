package runner;

import type.Node;
import type.PathTriple;
import type.Relationship;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/13 00:13
 * @Version 1.0
 */
public abstract class GraphDB {

    public abstract Optional<Node> nodeAt(Integer id);

    public abstract Iterator<Node> nodes();

    public Iterator<Node> nodes(NodeFilter nodeFilter) {
        return new Iterator<>() {
            private Iterator<Node> it = nodes();
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Node next() {
                Node node = it.next();
                return nodeFilter.match(node)?node:null;
            }
        };
    }

    public abstract Iterator<PathTriple> relationships();

    public Iterator<PathTriple> relationships(RelationshipFilter relationshipFilter) {
        return new Iterator<>() {
            private Iterator<PathTriple> it = relationships();
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public PathTriple next() {
                PathTriple pathTriple = it.next();
                return relationshipFilter.match(pathTriple.getStoreRelationship())?pathTriple:null;
            }
        };
    }

    public Iterator<PathTriple> relationships(NodeFilter startNodeFilter,NodeFilter endNodeFilter,RelationshipFilter relationshipFilter){
        return new Iterator<PathTriple>() {
            private Iterator<PathTriple> it = relationships();
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public PathTriple next() {
                PathTriple pathTriple = it.next();
                return relationshipFilter.match(pathTriple.getStoreRelationship())&&
                        startNodeFilter.match(pathTriple.getStartNode())&&
                        endNodeFilter.match(pathTriple.getEndNode())?pathTriple:null;
            }
        };
    }

    /*following methods are related to write task*/
    public abstract void deleteNodes(List<Integer> nodeIds);

    public abstract void deleteRelationships(List<Integer> relationshipIds);

    public abstract Node updateNode(Integer id, List<String> labels, Map<String, Object> prop);

    public abstract Relationship updateRelationship(Integer relationshipId, Map<String, Object> prop);

    public abstract List<Node> setNodesProperties(List<Integer> nodeIds, Map<String, Object> data, Boolean clearExistProperties);

    public abstract List<Node> setNodesLabels(List<Integer> nodeIds, List<String> labels);

    public abstract List<Relationship> setRelationshipsProperties(List<Integer> relationshipIds, Map<String, Object> data);

    public abstract List<Relationship> setRelationshipsType(List<Integer> relationshipIds, String typeName);

    public abstract List<Node> removeNodesProperties(List<Integer> nodeIds, List<String> keys);

    public abstract List<Node> removeNodesLabels(List<Integer> nodeIds, List<String> labels);

    public abstract List<Relationship> removeRelationshipsProperties(List<Integer> relationshipIds, List<String> keys);

    public abstract List<Relationship> removeRelationshipsType(List<Integer> relationshipIds, String typeName);
}
