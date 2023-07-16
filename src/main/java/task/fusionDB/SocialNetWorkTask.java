package task.fusionDB;

import lombok.extern.slf4j.Slf4j;
import runner.GraphDB;
import runner.NodeFilter;
import runner.RelationshipFilter;
import type.Node;
import type.PathTriple;
import util.CommonUtil;


import java.util.*;
import java.util.stream.Collectors;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/15 23:55
 * @Version 1.0
 */
@Slf4j
public class SocialNetWorkTask {

    private GraphDB neo4jDb;

    private static final Double IMAGE_SIMILAR = 0.98;

    public SocialNetWorkTask() {
        this.neo4jDb = new Neo4jDB();
    }

    /**
     * @desc return friends with certain name and photo
     * person->friend(photo:face,name)
     * @param firstName
     * @param facePath
     */
    public void t1(String firstName, String facePath) {
        long t1 = System.currentTimeMillis();
        NodeFilter nodeFilter = new NodeFilter();
        nodeFilter.setLabels(List.of("Person"));
        nodeFilter.setProperties(Map.of("firstName", firstName));
        List<Node> nodes = CommonUtil.convertIterator2List(neo4jDb.nodes(nodeFilter), e -> e != null&& AIService.similarity((String) e.property("face"),facePath)>IMAGE_SIMILAR);
        nodes.forEach(System.out::println);
        log.info("task 1 cost {} ms",(System.currentTimeMillis()-t1));
    }

    /**
     * @desc recent positive sentiment message from friends like
     * person(photo:face)->friend(photo:face,name)-[r:like]>message(text:sentiment)
     * @param personFace
     * @param friendFace
     * @param sentiment
     */
    public void t2(String personFace, String friendFace, int sentiment) {
        long t1 = System.currentTimeMillis();
        //1.获取person
        NodeFilter nodeFilter = new NodeFilter();
        nodeFilter.setLabels(List.of("Person"));
        List<Node> nodes = CommonUtil.convertIterator2List(neo4jDb.nodes(nodeFilter), e -> e != null && AIService.similarity((String) e.property("face"), personFace) > IMAGE_SIMILAR, 1);
        if (nodes == null || nodes.size() == 0) {
            log.error("task 2 doesn't find person:{}", personFace);
            return;
        }
        Node person = nodes.get(0);

        //2.获取指定face的friend
        RelationshipFilter knowsRelationshipFilter = new RelationshipFilter();
        knowsRelationshipFilter.setType("KNOWS");

        List<PathTriple> pathTriples = CommonUtil.convertIterator2List(neo4jDb.relationships(knowsRelationshipFilter), e -> e.getStartNode().getId().equals(person.getId())
                && AIService.similarity((String) e.getEndNode().property("face"), friendFace) > IMAGE_SIMILAR, 1);
        if (pathTriples == null || pathTriples.size() == 0) {
            log.info("task 2 doesn't find friend:{}", friendFace);
            return;
        }
        Node friend = pathTriples.get(0).getEndNode();

        //3.获取指定情感类型message
        RelationshipFilter likeRelationshipFilter = new RelationshipFilter();
        likeRelationshipFilter.setType("LIKES");
        NodeFilter messageFilter = new NodeFilter();
        messageFilter.setLabels(List.of("Comment"));
        List<PathTriple> comment = CommonUtil.convertIterator2List(neo4jDb.relationships(null, messageFilter, likeRelationshipFilter), e -> e.getStartNode().getId().equals(friend.getId() ) && AIService.classifySenti((String) e.getEndNode().property("content")) == sentiment);
        comment.forEach(e -> log.info((String) e.getEndNode().property("content")));
        log.info("task 2 cost {}ms", (System.currentTimeMillis() - t1));
    }


    /**
     * @desc Geolocation portrait Search
     * person->friend(photo:face)->city
     * @param personId
     * @param friendFace
     * @param cityId
     */
    public void t3(String personId, String friendFace, String cityId) {

        long t1 = System.currentTimeMillis();
        RelationshipFilter relationshipFilter = new RelationshipFilter();
        relationshipFilter.setType("KNOWS");
        List<PathTriple> pathTriples1 = CommonUtil.convertIterator2List(neo4jDb.relationships(relationshipFilter), e -> e != null && e.getStartNode().getId().equals(Integer.valueOf(personId)));


        relationshipFilter.setType("IS_LOCATED_IN");
        List<PathTriple> pathTriples2 = CommonUtil.convertIterator2List(neo4jDb.relationships(relationshipFilter), e -> e != null && e.getEndNode().getId().equals(Integer.valueOf(cityId)));

        List<Integer> friendIds = pathTriples1.stream().map(e -> e.getEndNode().getId()).collect(Collectors.toList());
        List<Node> result = pathTriples2.stream().map(e -> e.getStartNode()).filter(e -> friendIds.contains(e.getId()))
                .filter(e -> AIService.similarity((String) e.property("face"), friendFace) > IMAGE_SIMILAR).collect(Collectors.toList());
        long t2 = System.currentTimeMillis();
        log.info("task 3 cost {}ms", (t2 - t1));
        System.out.println(result);
    }

    /**
     * @desc Count the number of specific sentiment message of friend
     * person->friends->message(text:sentiment) friend,count(message)
     * @param personId
     * @param sentiment
     */
    public void t4(String personId, int sentiment) {
        /*朋友最近喜欢的积极消息的数量*/
        long t1 = System.currentTimeMillis();
        RelationshipFilter relationshipFilter = new RelationshipFilter();
        relationshipFilter.setType("KNOWS");
        Set<Node> friends = CommonUtil.convertIterator2List(neo4jDb.relationships(relationshipFilter), e -> e.getStartNode().getId().equals(Integer.valueOf(personId))).stream().map(e -> e.getEndNode()).collect(Collectors.toSet());

        for (Node f : friends) {
            NodeFilter startNodeFilter = new NodeFilter();
            startNodeFilter.setLabels(List.of("Person"));
            NodeFilter endNodeFilter = new NodeFilter();
            endNodeFilter.setLabels(List.of("Comment"));
            RelationshipFilter likeFilter = new RelationshipFilter();
            likeFilter.setType("LIKES");
            Iterator<PathTriple> creatorPathTriple = neo4jDb.relationships(startNodeFilter, endNodeFilter, likeFilter);
            int msgCount = CommonUtil.convertIterator2List(creatorPathTriple, e -> e.getStartNode().getId().equals(f.getId()) && AIService.classifySenti((String) e.getEndNode().property("content")) == sentiment).size();
            log.info("friend" + f.getId() + " has message:" + msgCount);
        }
        long t2 = System.currentTimeMillis();
        log.info("task 4 cost:{}ms", (t2 - t1));
    }

    /**
     * optimizer :1.早停  2. 批量提交文本  3. ANN相似向量搜索算法
     * @desc Recent positive message by friends or friends of friends create
     * @param face
     * @param sentiment
     */
    public void t5(String face, int sentiment) {
        long t1 = System.currentTimeMillis();
        NodeFilter nodeFilter = new NodeFilter();
        nodeFilter.setLabels(List.of("Person"));
        nodeFilter.setProperties(Map.of("face", face));
        Iterator<Node> nodeIterator = neo4jDb.nodes(nodeFilter);
        List<Node> nodeList = CommonUtil.convertIterator2List(nodeIterator, e -> e != null && AIService.similarity((String) e.property("face"), face) > IMAGE_SIMILAR, 1);

        if (nodeList == null || nodeList.size() == 0) {
            log.info("task5 doesn't find person with face:{}", face);
            return;
        }
        Node person = nodeList.get(0);
        RelationshipFilter relationshipFilter = new RelationshipFilter();
        relationshipFilter.setType("KNOWS");
        Iterator<PathTriple> pathTripleIterator = neo4jDb.relationships(relationshipFilter);
        Set<Node> friends = CommonUtil.convertIterator2List(pathTripleIterator, e -> e.getStartNode().getId().equals(person.getId())).stream().map(e -> e.getEndNode()).collect(Collectors.toSet());

        if (friends.size() == 0) {
            log.info("In task 5, person with face:{} doesn't has any friends");
            return;
        }

        for (Node f : friends) {
            NodeFilter startNodeFilter = new NodeFilter();
            startNodeFilter.setLabels(List.of("Comment"));
            NodeFilter endNodeFilter = new NodeFilter();
            RelationshipFilter creatorFilter = new RelationshipFilter();
            creatorFilter.setType("HAS_CREATOR");
            Iterator<PathTriple> creatorPathTriple = neo4jDb.relationships(startNodeFilter, endNodeFilter, creatorFilter);
            Set<Node> message = CommonUtil.convertIterator2List(creatorPathTriple, e -> e.getEndNode().getId().equals(f.getId()) && AIService.classifySenti((String) e.getStartNode().property("content")) == sentiment).stream().map(e -> e.getStartNode()).collect(Collectors.toSet());
            log.info("friend:{} has {} messages,sentiment {}", f.getId(), message.size(), sentiment);
        }
        long t2 = System.currentTimeMillis();
        log.info("task 5 cost {}ms", (t2 - t1));
    }


    /**
     * 最短路径
     * @param personId
     * @param friendFacePath
     */
    public void t6(String personId,String friendFacePath){
        long t1 = System.currentTimeMillis();
        /*方法1：暴力*/
        //1。获取所有的可能friends
        NodeFilter nodeFilter = new NodeFilter();
        nodeFilter.setLabels(List.of("Person"));
        List<Node> nodes = CommonUtil.convertIterator2List(neo4jDb.nodes(nodeFilter), e -> AIService.similarity((String) e.property("face"), friendFacePath) > IMAGE_SIMILAR, 3);

        log.info("find {} possible path",nodes.size());
        for(int i=0;i<nodes.size();i++){
            List<Node> nodePath = CommonUtil.convertIterator2List(neo4jDb.shortestPath(personId, String.valueOf(nodes.get(i).getId()), "KNOWS"), node -> node != null);
            log.info("path {}/{} length:{}",i+1,nodes.size(),nodePath.size());
        }
        long t2 = System.currentTimeMillis();
        log.info("task 6 cost {} ms",(t2-t1));
    }


    public static void main(String[] args) {
        SocialNetWorkTask socialNetWorkTask = new SocialNetWorkTask();
//        socialNetWorkTask.t1("Miguel", "/Users/along/Documents/dataset/FaceDataset/lfw/Michael_Bouchard/Michael_Bouchard_0001.jpg");
//        socialNetWorkTask.t2("/Users/along/Documents/dataset/FaceDataset/lfw/Michael_Bouchard/Michael_Bouchard_0001.jpg", "/Users/along/Documents/dataset/FaceDataset/lfw/Queen_Elizabeth_II/Queen_Elizabeth_II_0009.jpg", 2);
//        socialNetWorkTask.t3("1711", "/Users/along/Documents/dataset/FaceDataset/lfw/Margaret_Okayo/Margaret_Okayo_0001.jpg", "5805");
//        socialNetWorkTask.t4("1714",2);
//        socialNetWorkTask.t5("/Users/along/Documents/dataset/FaceDataset/lfw/Margaret_Okayo/Margaret_Okayo_0001.jpg", 2);
        socialNetWorkTask.t6("1709","/Users/along/Documents/dataset/FaceDataset/lfw/Hector_Grullon/Hector_Grullon_0001.jpg");
    }


}


