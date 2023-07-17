package task.fusionDB;

import lombok.extern.slf4j.Slf4j;
import runner.GraphDB;
import runner.NodeFilter;
import runner.RelationshipFilter;
import type.Node;
import type.PathTriple;
import util.CommonUtil;


import java.sql.SQLSyntaxErrorException;
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
     * @param firstName
     * @param facePath
     * @desc return friends with certain name and photo
     * person->friend(photo:face,name)
     */
    public void t1(String personId, String firstName, String facePath) {
        long t1 = System.currentTimeMillis();
        NodeFilter nodeFilter = new NodeFilter();
        nodeFilter.setLabels(List.of("Person"));
        nodeFilter.setProperties(Map.of("firstName", firstName));
        List<Node> nodes = CommonUtil.convertIterator2List(neo4jDb.nodes(nodeFilter), e -> e != null && AIService.similarity((String) e.property("face"), facePath) > IMAGE_SIMILAR);
        log.info("task 1 cost {} ms\n\n", (System.currentTimeMillis() - t1));
    }

    /**
     * @param personFace
     * @param friendFace
     * @param sentiment
     * @desc recent positive sentiment message from friends like
     * person(photo:face)->friend(photo:face,name)-[r:like]>message(text:sentiment)
     */
    public void t2(String personFace, String friendFace, int sentiment) {
        long t1 = System.currentTimeMillis();
        //1.获取person
        NodeFilter personFilter = new NodeFilter();
        personFilter.setLabels(List.of("Person"));
        List<Node> nodes = CommonUtil.convertIterator2List(neo4jDb.nodes(personFilter), e -> e != null && AIService.similarity((String) e.property("face"), personFace) > IMAGE_SIMILAR, 1);
        if (nodes == null || nodes.size() == 0) {
            log.error("task 2 doesn't find person:{}", personFace);
            return;
        }
        Node person = nodes.get(0);
        personFilter.setId(String.valueOf(person.getId()));

        //2.获取指定face的friend
        RelationshipFilter knowsRelationshipFilter = new RelationshipFilter();
        knowsRelationshipFilter.setType("KNOWS");

        List<PathTriple> pathTriples = CommonUtil.convertIterator2List(neo4jDb.relationships(personFilter, null, knowsRelationshipFilter), e -> AIService.similarity((String) e.getEndNode().property("face"), friendFace) > IMAGE_SIMILAR, 1);
        if (pathTriples == null || pathTriples.size() == 0) {
            log.info("task 2 doesn't find friend:{}", friendFace);
            return;
        }
        Node friend = pathTriples.get(0).getEndNode();
        personFilter.setId(String.valueOf(friend.getId()));

        //3.获取指定情感类型message
        RelationshipFilter likeRelationshipFilter = new RelationshipFilter();
        likeRelationshipFilter.setType("LIKES");
        NodeFilter messageFilter = new NodeFilter();
        messageFilter.setLabels(List.of("Comment"));
        List<Node> comments = CommonUtil.convertIterator2List(neo4jDb.relationships(personFilter, messageFilter, likeRelationshipFilter), e -> e != null)
                .stream()
                .sorted((e1, e2) -> {
                    long a = (long) e2.getEndNode().property("creationDate");
                    long b = (long) e1.getEndNode().property("creationDate");
                    return (int) (a - b);
                })
                .filter(e -> AIService.classifySenti((String) e.getEndNode().property("content")) == sentiment)
                .map(e->e.getEndNode())
                .collect(Collectors.toList())
                .subList(0, 10);
        comments.forEach(e -> log.info("friend id:{},firstName:{},lastName:{}. Message content:{},creationDate:{}",
                friend.getId(), friend.property("firstName"), friend.property("lastName"), e.property("content"), e.property("creationDate")));
        log.info("task 2 cost {}ms\n\n", (System.currentTimeMillis() - t1));
    }


    /**
     * @param personId
     * @param friendFace
     * @param cityId
     * @desc Geolocation portrait Search
     * person->friend(photo:face)->city
     */
    public void t3(String personId, String friendFace, String cityId) {

        long t1 = System.currentTimeMillis();
        RelationshipFilter relationshipFilter = new RelationshipFilter();
        relationshipFilter.setType("KNOWS");

        NodeFilter personFilter = new NodeFilter();
        personFilter.setId(personId);
        personFilter.setLabels(List.of("Person"));
        List<PathTriple> pathTriples1 = CommonUtil.convertIterator2List(neo4jDb.relationships(personFilter, null, relationshipFilter), e -> e != null);


        relationshipFilter.setType("IS_LOCATED_IN");
        NodeFilter cityFilter = new NodeFilter();
        cityFilter.setId(cityId);
        cityFilter.setLabels(List.of("City"));
        List<PathTriple> pathTriples2 = CommonUtil.convertIterator2List(neo4jDb.relationships(null, cityFilter, relationshipFilter), e -> e != null);

        List<Integer> friendIds = pathTriples1.stream().map(e -> e.getEndNode().getId()).collect(Collectors.toList());
        List<Node> result = pathTriples2.stream().map(e -> e.getStartNode()).filter(e -> friendIds.contains(e.getId()))
                .filter(e -> AIService.similarity((String) e.property("face"), friendFace) > IMAGE_SIMILAR).collect(Collectors.toList());
        long t2 = System.currentTimeMillis();
        log.info("find person:{} in city:{} and face:{}", result, cityId, friendFace);
        log.info("task 3 cost {}ms\n\n", (t2 - t1));
    }

    /**
     * @param personId
     * @param sentiment
     * @desc Count the number of specific sentiment messages which friend likes
     * person->friends->message(text:sentiment) friend,count(message)
     */
    public void t4(String personId, int sentiment) {
        /*朋友最近喜欢的积极消息的数量*/
        long t1 = System.currentTimeMillis();
        RelationshipFilter relationshipFilter = new RelationshipFilter();
        relationshipFilter.setType("KNOWS");

        NodeFilter personFilter = new NodeFilter();
        personFilter.setLabels(List.of("Person"));
        personFilter.setId(personId);
        Set<Node> friends = CommonUtil.convertIterator2List(neo4jDb.relationships(personFilter, null, relationshipFilter), e -> e != null).stream().map(e -> e.getEndNode()).collect(Collectors.toSet());

        NodeFilter commentNodeFilter = new NodeFilter();
        commentNodeFilter.setLabels(List.of("Comment"));
        RelationshipFilter likeFilter = new RelationshipFilter();
        likeFilter.setType("LIKES");
        for (Node f : friends) {
            personFilter.setId(String.valueOf(f.getId()));
            Iterator<PathTriple> creatorPathTriple = neo4jDb.relationships(personFilter, commentNodeFilter, likeFilter);
            int msgCount = CommonUtil.convertIterator2List(creatorPathTriple, e -> AIService.classifySenti((String) e.getEndNode().property("content")) == sentiment).size();
            log.info("friend:{} has message:{}", f.getId(), msgCount);
        }
        long t2 = System.currentTimeMillis();
        log.info("task 4 cost:{}ms\n\n", (t2 - t1));
    }

    /**
     * optimizer :1.早停  2. 批量提交文本  3. ANN相似向量搜索算法
     *
     * @param personFace
     * @param sentiment
     * @desc Recent negative message by friends or friends of friends create
     */
    public void t5(String personFace, int sentiment) {
        long t1 = System.currentTimeMillis();
        NodeFilter personFilter = new NodeFilter();
        personFilter.setLabels(List.of("Person"));
        Iterator<Node> nodeIterator = neo4jDb.nodes(personFilter);
        List<Node> nodeList = CommonUtil.convertIterator2List(nodeIterator, e -> e != null && AIService.similarity((String) e.property("face"), personFace) > IMAGE_SIMILAR, 1);

        if (nodeList == null || nodeList.size() == 0) {
            log.info("task5 doesn't find person with face:{}", personFace);
            return;
        }
        Node person = nodeList.get(0);
        RelationshipFilter relationshipFilter = new RelationshipFilter();
        relationshipFilter.setType("KNOWS");
        personFilter.setId(String.valueOf(person.getId()));

        Iterator<PathTriple> pathTripleIterator = neo4jDb.relationships(personFilter, null, relationshipFilter, 1, 2);
        Set<Node> friends = CommonUtil.convertIterator2List(pathTripleIterator, e -> e != null).stream().map(e -> e.getEndNode()).collect(Collectors.toSet());

        if (friends.size() == 0) {
            log.info("In task 5, person with face:{} doesn't has any friends");
            return;
        }

        for (Node f : friends) {
            NodeFilter startNodeFilter = new NodeFilter();
            startNodeFilter.setLabels(List.of("Comment"));
            RelationshipFilter creatorFilter = new RelationshipFilter();
            creatorFilter.setType("HAS_CREATOR");
            personFilter.setId(String.valueOf(f.getId()));
            Iterator<PathTriple> creatorPathTriple = neo4jDb.relationships(startNodeFilter, personFilter, creatorFilter);
            List<Node> message = CommonUtil.convertIterator2List(creatorPathTriple, e -> e!=null)
                    .stream()
                    .sorted((e1,e2)->{
                        long a = (long)e2.getEndNode().property("creationDate");
                        long b = (long)e1.getEndNode().property("creationDate");
                        return (int)(a-b);
                    })
                    .filter(e->AIService.classifySenti((String) e.getStartNode().property("content")) == sentiment)
                    .map(e->e.getStartNode())
                    .collect(Collectors.toList())
                    .subList(0,10);
            log.info("friend id:{},firstName:{},lastName:{} has {} messages,sentiment {}", f.getId(), message.size(), sentiment);
        }
        long t2 = System.currentTimeMillis();
        log.info("task 5 cost {}ms\n\n", (t2 - t1));
    }


    /**
     * @param personId
     * @param friendFacePath
     * @desc 最短路径
     */
    public void t7(String personId, String friendFacePath) {
        long t1 = System.currentTimeMillis();
        /*方法1：暴力*/
        //1。获取所有的可能friends
        NodeFilter nodeFilter = new NodeFilter();
        nodeFilter.setLabels(List.of("Person"));

        List<Node> nodes = CommonUtil.convertIterator2List(neo4jDb.nodes(nodeFilter), e -> AIService.similarity((String) e.property("face"), friendFacePath) > IMAGE_SIMILAR, 3);

        log.info("find {} possible path", nodes.size());
        RelationshipFilter relationshipFilter = new RelationshipFilter();
        relationshipFilter.setType("KNOWS");
        for (int i = 0; i < nodes.size(); i++) {
            List<Node> nodePath = CommonUtil.convertIterator2List(neo4jDb.shortestPath(personId, String.valueOf(nodes.get(i).getId()), relationshipFilter), node -> node != null);
            log.info("path {}/{} length:{}", i + 1, nodes.size(), nodePath.size());
        }
        long t2 = System.currentTimeMillis();
        log.info("task 6 cost {} ms\n\n", (t2 - t1));
    }

    /*=======================short read==========================*/

    /**
     * person profile
     *
     * @param personFace
     */
    public void t8(String personFace) {
        long t1 = System.currentTimeMillis();
        NodeFilter personFilter = new NodeFilter();
        personFilter.setLabels(List.of("Person"));

        List<Node> nodes = CommonUtil.convertIterator2List(neo4jDb.nodes(personFilter), e -> AIService.similarity((String) e.property("face"), personFace) > IMAGE_SIMILAR, 1);
        if (nodes == null || nodes.size() == 0) {
            log.warn("task 7 not find person {}", personFace);
            return;
        }

        Node person = nodes.get(0);

        RelationshipFilter relationshipFilter = new RelationshipFilter();
        relationshipFilter.setType("IS_LOCATED_IN");

        NodeFilter cityFilter = new NodeFilter();
        cityFilter.setLabels(List.of("City"));

        List<PathTriple> pathTriples = CommonUtil.convertIterator2List(neo4jDb.relationships(personFilter, cityFilter, relationshipFilter, 1, 1), e -> e.getStartNode().getId().equals(person.getId()));
        if (pathTriples == null | pathTriples.size() == 0) {
            log.warn("task 7 not find city of residence of person {}", personFace);
            return;
        }
        Node city = pathTriples.get(0).getEndNode();
        log.info("person{firstName:{},lastName:{},browser:{}},city{name:{}}", person.property("firstName"), person.property("lastName"), person.property("browserUsed"), city.property("name"));
        long t2 = System.currentTimeMillis();
        log.info("task 7 cost {} ms\n\n", (t2 - t1));
    }


    /**
     * @param personFace
     * @desc friends of a person.retrieve all of their friends(id,firstName,lastName), and the date at which they became friends.
     */
    public void t9(String personFace) {
        long t1 = System.currentTimeMillis();
        NodeFilter personFilter = new NodeFilter();
        personFilter.setLabels(List.of("Person"));
        List<Node> nodes = CommonUtil.convertIterator2List(neo4jDb.nodes(personFilter), e -> AIService.similarity((String) e.property("face"), personFace) > IMAGE_SIMILAR, 1);
        if (nodes == null || nodes.size() == 0) {
            log.warn("task 8 not find person {}", personFace);
            return;
        }
        Node person = nodes.get(0);
        RelationshipFilter knowsFilter = new RelationshipFilter();
        knowsFilter.setType("KNOWS");
        List<PathTriple> friends = CommonUtil.convertIterator2List(neo4jDb.relationships(knowsFilter), e -> e.getStartNode().getId().equals(person.getId()));
        friends.forEach(e -> {
            log.info("person {} and person {} become friend in {}", person.getId(), e.getEndNode().getId(), e.getStoreRelationship().property("creationDate"));
        });
        long t2 = System.currentTimeMillis();
        log.info("task 8 cost {} ms\n\n", (t2 - t1));
    }

    /**
     * @param messageId
     * @Desc sentiment of Comment, give Comment id,return its sentiment
     */
    public void t10(String messageId) {
        long t1 = System.currentTimeMillis();
        NodeFilter nodeFilter = new NodeFilter();
        nodeFilter.setId(messageId);
        nodeFilter.setLabels(List.of("Comment"));
        List<Node> nodes = CommonUtil.convertIterator2List(neo4jDb.nodes(nodeFilter), e -> e != null);
        if (nodes == null || nodes.size() == 0) {
            log.info("task 9 not find message {}\n\n", messageId);
            return;
        }

        Node message = nodes.get(0);
        int sentiment = AIService.classifySenti((String) message.property("content"));
        log.info("sentiment of message {} is {}", messageId, sentiment);
        long t2 = System.currentTimeMillis();
        log.info("task 9 cost {} ms\n\n", (t2 - t1));
    }

    /**
     * give a personID, return  sentiment distribution of last 10 last message
     *
     * @param personId
     */
    public void t11(String personId) {
        long t1 = System.currentTimeMillis();
        NodeFilter personFilter = new NodeFilter();
        personFilter.setLabels(List.of("Person"));
        personFilter.setId(personId);

        NodeFilter messageFilter = new NodeFilter();
        messageFilter.setLabels(List.of("Message", "Comment"));

        RelationshipFilter relationshipFilter = new RelationshipFilter();
        relationshipFilter.setType("HAS_CREATOR");

        List<PathTriple> pathTriples = CommonUtil.convertIterator2List(neo4jDb.relationships(messageFilter, personFilter, relationshipFilter), e -> e != null)
                .stream()
                .sorted((e1, e2) -> {
                    long a = (long) e2.getStoreRelationship().property("creationDate");
                    long b = (long) e1.getStoreRelationship().property("creationDate");
                    return (int) (a - b);
                })
                .collect(Collectors.toList())
                .subList(0, 10);

        int negativeCnt = 0;
        int positiveCnt = 0;
        int neutralCnt = 0;
        for (PathTriple pathTriple : pathTriples) {
            Node comment = pathTriple.getStartNode();
            int sentiment = AIService.classifySenti((String) comment.property("content"));
            if (sentiment == 0) {
                negativeCnt++;
            } else if (sentiment == 1) {
                neutralCnt++;
            } else if (sentiment == 2) {
                positiveCnt++;
            }
        }
        log.info("person {} has {} positive message in last {} message", personId, positiveCnt, pathTriples.size());
        log.info("person {} has {} negative message in last {} message", personId, negativeCnt, pathTriples.size());
        log.info("person {} has {} neutral message in last {} message", personId, neutralCnt, pathTriples.size());
        long t2 = System.currentTimeMillis();
        log.info("task 10 cost {} ms", (t2 - t1));
    }


    public static void main(String[] args) {
        SocialNetWorkTask socialNetWorkTask = new SocialNetWorkTask();
        log.info("===========complex read===========");
//        socialNetWorkTask.t1("Miguel", "/Users/along/Documents/dataset/FaceDataset/lfw/Michael_Bouchard/Michael_Bouchard_0001.jpg");
//        socialNetWorkTask.t2("/Users/along/Documents/dataset/FaceDataset/lfw/Michael_Bouchard/Michael_Bouchard_0001.jpg", "/Users/along/Documents/dataset/FaceDataset/lfw/Queen_Elizabeth_II/Queen_Elizabeth_II_0009.jpg", 2);
//        socialNetWorkTask.t3("1711", "/Users/along/Documents/dataset/FaceDataset/lfw/Margaret_Okayo/Margaret_Okayo_0001.jpg", "6351");
//        socialNetWorkTask.t4("1714", 2);
//        socialNetWorkTask.t5("/Users/along/Documents/dataset/FaceDataset/lfw/Margaret_Okayo/Margaret_Okayo_0001.jpg", 0);
        socialNetWorkTask.t7("1709", "/Users/along/Documents/dataset/FaceDataset/lfw/Hector_Grullon/Hector_Grullon_0001.jpg");

//        log.info("===========short read===========");
//        socialNetWorkTask.t8("/Users/along/Documents/dataset/FaceDataset/lfw/Islam_Karimov/Islam_Karimov_0001.jpg");
//        socialNetWorkTask.t9("/Users/along/Documents/dataset/FaceDataset/lfw/Bruce_Van_De_Velde/Bruce_Van_De_Velde_0002.jpg");
//        socialNetWorkTask.t10("241");
//        socialNetWorkTask.t11("1709");
    }


}


