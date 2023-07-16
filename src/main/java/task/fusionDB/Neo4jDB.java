package task.fusionDB;

import org.apache.log4j.Logger;
import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import runner.GraphDB;
import runner.NodeFilter;
import runner.RelationshipFilter;
import type.Node;
import type.PathTriple;
import type.Relationship;
import util.CommonUtil;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/20 23:05
 * @Version 1.0
 */
public class Neo4jDB extends GraphDB {

    Logger logger = Logger.getLogger(Neo4jDB.class);

    private final Driver driver;

    private String uri = "bolt://localhost:7687";

    private String user = "neo4j";

    private String pwd = "neo4j";

    public Neo4jDB() {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, pwd));
    }


    @Override
    public Optional<Node> nodeAt(Integer id) {
        Node node = null;
        try (Session session = driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                org.neo4j.driver.types.Node neo4jNode = tx.run("match (n) where $nodeId = id(n) return n", Map.of("nodeId", id)).single().get("n").asNode();
                node = new Node();
                node.setId(id);
                node.setLabels(CommonUtil.convertIterator2List(neo4jNode.labels().iterator(), e -> e != null));
                node.setProperties(neo4jNode.asMap());
            } catch (NoSuchRecordException e) {
                logger.error(String.format("node not found,nodeID:{}", id), e);
            }
        }
        return Optional.ofNullable(node);
    }

    @Override
    public Iterator<Node> nodes() {
        return new Iterator<>() {
            Session session = driver.session();
            Transaction tx = session.beginTransaction();
            Result rs = tx.run("match (n) return n");

            @Override
            public boolean hasNext() {
                if (rs.hasNext()) {
                    return true;
                }
                tx.close();
                session.close();
                return false;
            }

            @Override
            public Node next() {
                org.neo4j.driver.types.Node neo4jNode = rs.next().get("n").asNode();
                return neo4jNode2Node(neo4jNode);
            }
        };
    }


    @Override
    public Iterator<Node> nodes(NodeFilter nodeFilter) {
        String filterStr = getNodeFilterStr(nodeFilter);
        Session session = driver.session();
        Transaction tx = session.beginTransaction();
        /*先过滤结构化属性*/
        Result rs = tx.run(String.format("match (n%s) return n", filterStr), nodeFilter.getProperties());
//        List<String> intelPropertyKey = nodeFilter.getProperties().keySet().stream().filter(this::isIntelligentAttr).collect(Collectors.toList());
        return new Iterator<>() {
            //            boolean earlyStop = false;
            @Override
            public boolean hasNext() {
                if (rs.hasNext()) {
                    return true;
                }
                tx.close();
                session.close();
                return false;
            }

            @Override
            public Node next() {
                long t1 = System.currentTimeMillis();
                org.neo4j.driver.types.Node neo4jNode = rs.next().get("n").asNode();
                logger.info("nodes(nodeFilter) get neo4j node cost " + (System.currentTimeMillis() - t1) + "ms");
                //TODO 统一非结构化属性过滤
//                if (intelPropertyKey.contains("face") && neo4jNode.asMap().containsKey("face")) {
//                    t1 = System.currentTimeMillis();
//                    String origin = (String) neo4jNode.asMap().get("face");
//                    String target = (String) nodeFilter.getProperties().get("face");
//                    Double similarity = AIPM.similarity(origin, target);
//                    long t2 = System.currentTimeMillis();
//                    logger.info("face filter cost " + (t2 - t1) + "ms");
//                    if (similarity < 0.9) {
//                        return null;
//                    } else {
//                        earlyStop = true;
//                    }
//                }
                return neo4jNode2Node(neo4jNode);
            }
        };
    }


    @Override
    public Iterator<PathTriple> relationships() {
        return null;
    }

    @Override
    public Iterator<PathTriple> relationships(RelationshipFilter relationshipFilter) {
        return relationships(null,null,relationshipFilter,1,1);
    }


    @Override
    public Iterator<PathTriple> relationships(NodeFilter startNodeFilter, NodeFilter endNodeFilter, RelationshipFilter relationshipFilter) {
        return relationships(startNodeFilter,endNodeFilter,relationshipFilter,1,1);
    }

    @Override
    public Iterator<PathTriple> relationships(NodeFilter startNodeFilter, NodeFilter endNodeFilter, RelationshipFilter relationshipFilter, int from, int to) {
        String startNodeFilterStr = getNodeFilterStr(startNodeFilter);
        String endNodeFilterStr = getNodeFilterStr(endNodeFilter);
        String relFilterStr = getRelFilterStr(relationshipFilter);

        Session session = driver.session();
        Transaction tx = session.beginTransaction();

        Map<String, Object> param = new HashMap<>();
        if (startNodeFilter != null && startNodeFilter.getProperties() != null) {
            param.putAll(startNodeFilter.getProperties());
        }
        if (endNodeFilter != null && endNodeFilter.getProperties() != null) {
            param.putAll(endNodeFilter.getProperties());
        }
        if (relationshipFilter != null && relationshipFilter.getProperties() != null) {
            param.putAll(relationshipFilter.getProperties());
        }

        String hop = from == to ? "from" : from + ".." + to;
        Result rs = tx.run(String.format("match (n%s)-[r:%s*%s]->(m%s) return n,r,m", startNodeFilterStr, relFilterStr, hop, endNodeFilterStr), param);
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                if (rs.hasNext()) {
                    return true;
                }
                tx.close();
                session.close();
                return false;
            }

            @Override
            public PathTriple next() {
                Record record = rs.next();
                org.neo4j.driver.types.Relationship neo4jRelationship = record.get("r").asRelationship();
                org.neo4j.driver.types.Node n = record.get("n").asNode();
                org.neo4j.driver.types.Node m = record.get("m").asNode();
                return new PathTriple(neo4jNode2Node(n), neo4jNode2Node(m), neo4jRel2Rel(neo4jRelationship), false);
            }
        };
    }

    @Override
    public Iterator<Node> shortestPath(String startNodeInnerId, String endNodeInnerId, String relationshipType) {
        // 定义Cypher查询

        String cypherQuery = String.format("MATCH path=shortestPath((startNode)-[:%s*]-(endNode))\n" +
                "WHERE id(startNode) = %s AND id(endNode) = %s\n" +
                "UNWIND nodes(path) AS n\n" +
                "RETURN n", relationshipType, startNodeInnerId, endNodeInnerId);

        // 运行查询
        Session session = driver.session();
        Transaction tx = session.beginTransaction();
        Result rs = tx.run(cypherQuery);
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                if (rs.hasNext()) {
                    return true;
                }
                tx.close();
                session.close();
                return false;
            }

            @Override
            public Node next() {
                org.neo4j.driver.types.Node neo4jNode = rs.next().get("n").asNode();
                return neo4jNode2Node(neo4jNode);
            }
        };
    }

    @Override
    public void deleteNodes(List<Integer> nodeIds) {

    }

    @Override
    public void deleteRelationships(List<Integer> relationshipIds) {

    }

    @Override
    public Node updateNode(Integer id, List<String> labels, Map<String, Object> prop) {
        return null;
    }

    @Override
    public Relationship updateRelationship(Integer relationshipId, Map<String, Object> prop) {
        return null;
    }

    @Override
    public List<Node> setNodesProperties(List<Integer> nodeIds, Map<String, Object> data, Boolean clearExistProperties) {
        return null;
    }

    @Override
    public List<Node> setNodesLabels(List<Integer> nodeIds, List<String> labels) {
        return null;
    }

    @Override
    public List<Relationship> setRelationshipsProperties(List<Integer> relationshipIds, Map<String, Object> data) {
        return null;
    }

    @Override
    public List<Relationship> setRelationshipsType(List<Integer> relationshipIds, String typeName) {
        return null;
    }

    @Override
    public List<Node> removeNodesProperties(List<Integer> nodeIds, List<String> keys) {
        return null;
    }

    @Override
    public List<Node> removeNodesLabels(List<Integer> nodeIds, List<String> labels) {
        return null;
    }

    @Override
    public List<Relationship> removeRelationshipsProperties(List<Integer> relationshipIds, List<String> keys) {
        return null;
    }

    @Override
    public List<Relationship> removeRelationshipsType(List<Integer> relationshipIds, String typeName) {
        return null;
    }

    protected Boolean isIntelligentAttr(String key) {
        return key.equals("face") || key.equals("content");
    }

    private Node neo4jNode2Node(org.neo4j.driver.types.Node neo4jNode) {
        Node node = new Node();
        node.setId((int) neo4jNode.id());
        node.setLabels(CommonUtil.convertIterator2List(neo4jNode.labels().iterator(), e -> e != null));
        node.setKeyList(neo4jNode.asMap().keySet().stream().filter(e -> !this.isIntelligentAttr(e)).collect(Collectors.toSet()));
        node.setIntelKeyList(neo4jNode.asMap().keySet().stream().filter(this::isIntelligentAttr).collect(Collectors.toSet()));
        node.setProperties(neo4jNode.asMap());
        return node;
    }

    private Relationship neo4jRel2Rel(org.neo4j.driver.types.Relationship neo4jRel) {
        Relationship relationship = new Relationship();
        relationship.setId((int) neo4jRel.id());
        relationship.setStartId((int) neo4jRel.startNodeId());
        relationship.setEndId((int) neo4jRel.endNodeId());
        relationship.setRelationshipType(neo4jRel.type());
        relationship.setKeyList(neo4jRel.asMap().keySet().stream().filter(e -> !this.isIntelligentAttr(e)).collect(Collectors.toSet()));
        relationship.setIntelKeyList(neo4jRel.asMap().keySet().stream().filter(this::isIntelligentAttr).collect(Collectors.toSet()));
        relationship.setProperties(neo4jRel.asMap());
        return relationship;
    }

    private String getNodeFilterStr(NodeFilter nodeFilter) {
        if (nodeFilter == null) {
            return "";
        }
        String labels = "";
        if (null != nodeFilter.getLabels()) {
            for (String label : nodeFilter.getLabels()) {
                labels += ":" + label;
            }
        }
        String attr = getPropertyFilterStr(nodeFilter.getProperties());
        return labels + attr;
    }

    private String getRelFilterStr(RelationshipFilter relationshipFilter) {
        if (relationshipFilter == null) {
            return "";
        }
        String type = relationshipFilter.getType();
        String attr = getPropertyFilterStr(relationshipFilter.getProperties());
        return type + attr;
    }

    /**
     * 获取非结构化属性过滤的字符串
     *
     * @param property
     * @return
     */
    private String getPropertyFilterStr(Map<String, Object> property) {
        String attr = "";
        if (property != null && property.size() > 0) {
            List<String> propertyKey = property.keySet().stream().filter(e -> !this.isIntelligentAttr(e)).collect(Collectors.toList());

            for (int i = 0; i < propertyKey.size(); i++) {
                attr += String.format("%s:$%s", propertyKey.get(i), propertyKey.get(i));
                if (i != propertyKey.size() - 1) {
                    attr += ",";
                }
            }
            attr = "{" + attr + "}";
        }
        return attr;
    }
}
