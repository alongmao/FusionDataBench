package task.fusionDB;

import org.apache.spark.internal.config.R;
import org.neo4j.driver.*;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.value.IntegerValue;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Tasks {
    public  final Double IMAGE_SIMILAR = 0.98;
    Driver driver;

    public Tasks(Driver driver) {
        this.driver = driver;
    }

    class Result {
        long time;
        long structuredTime;
        long unstructuredTime;
        double acc;

        public Result() {
        }

        public Result(long time, double acc) {
            this.time = time;
            this.acc = acc;
        }

        public Result(long time, long structuredTime, long unstructuredTime, double acc) {
            this.time = time;
            this.structuredTime = structuredTime;
            this.unstructuredTime = unstructuredTime;
            this.acc = acc;
        }
    }

    public  Result task1(List<String[]> parameterss){
        Session session = driver.session();
        int taskTimes = parameterss.size();
        long allTime = 0;
        long STime = 0;
        long USTime = 0;
        double acc = 0;

        for (String[] parameters : parameterss) {
            // return persons;
            long TIME_INIT = System.currentTimeMillis();
            List<Record> records = session.run("match (p:Person{id:"+parameters[0]+"})-[:KNOWS*1..3]->(friend:Person{firstName:'"+ parameters[1] +"'})" +
                    "return friend.id, friend.face").list();
            long TIME_S_END = System.currentTimeMillis();
            // filter by face
            List<Record> records1 = records.stream().filter(record ->
                            AIService.similarity(record.get(1).asString(), parameters[2]) >= IMAGE_SIMILAR)
                    .collect(Collectors.toList());
            long TIME_US_END = System.currentTimeMillis();
            // query other
            long[] ids = records1.stream().mapToLong(record -> record.get(0).asLong()).toArray();
            session.run("match (f:Person)-[:IS_LOCATED_IN]->(c:City) " +
                    " optional match (f:Person)-[:WORK_AT]->(co:Company)" +
                    " optional match (f:Person)-[:STUDY_AT]->(u:University)" +
                    " where f.id in [$ids]" +
                    " return c.name as cities, co.name as companies, u.name as universities", Map.of("ids", ids));
            long TIME_END = System.currentTimeMillis();
            allTime += TIME_END - TIME_INIT;
            STime += TIME_S_END - TIME_INIT;
            STime += TIME_END - TIME_US_END;
            USTime += TIME_US_END - TIME_S_END;
            long rightNum = records.stream().filter(record -> record.get(1).asString().equals(parameters[2])).count();
            long actualNum = records1.stream().filter(record -> record.get(1).asString().equals(parameters[2])).count();
            acc += (actualNum==rightNum?1:0);
        }
        session.close();
        return new Result(allTime/taskTimes, STime/taskTimes, USTime/taskTimes, acc/taskTimes);
    }

    public  Result task2(){
        Session session = driver.session();
        int taskTimes = parameters2.size();
        long allTime = 0;
        long STime = 0;
        long USTime = 0;
        double acc = 0;

        for (String[] parameters : parameters2) {
            // 1. return persons;
            long TIME_INIT = System.currentTimeMillis();
            List<Record> records = session.run("match (p:Person)" +
                    "return p.id, p.face").list();
            long T1 = System.currentTimeMillis();

            // 2. filter by face
            List<Record> P_F = records.stream().filter(record ->
                            AIService.similarity(record.get(1).asString(), parameters[0]) >= IMAGE_SIMILAR ||
                            AIService.similarity(record.get(1).asString(), parameters[1]) >= IMAGE_SIMILAR )
                    .collect(Collectors.toList());
            List<Record> P = P_F.stream().filter(record ->
                    AIService.similarity(record.get(1).asString(), parameters[0]) >= IMAGE_SIMILAR).collect(Collectors.toList());
            List<Record> F = P_F.stream().filter(record ->
                    AIService.similarity(record.get(1).asString(), parameters[1]) >= IMAGE_SIMILAR).collect(Collectors.toList());
            long T2 = System.currentTimeMillis();

            // 3. match
            long[] pids = P.stream().mapToLong(record -> record.get(0).asLong()).toArray();
            long[] fids = F.stream().mapToLong(record -> record.get(0).asLong()).toArray();

            List<Record> comments = session.run("match (p:Person)-[:KNOWS]->(f:Person)-[:LIKES]->(m:Comment) " +
                    "where p.id in $pids and f.id in $fids " +
                    "return f.id,f.firstName,f.lastName,m.content,m.creationDate,m.id order by m.creationDate desc",
                    Map.of("pids", pids,
                            "fids", fids)).list();
            long T3 = System.currentTimeMillis();

            // 4. check type
            List<Record> result = comments.stream()
                    .filter(record -> AIService.classifySenti(record.get(3).asString())==Integer.parseInt(parameters[2]))
                    .limit(10)
                    .collect(Collectors.toList());
            long TIME_END = System.currentTimeMillis();

            allTime += TIME_END - TIME_INIT;
            STime += T1 - TIME_INIT; // 1
            USTime += T2 - T1; // 2
            STime += T3 - T2; // 3
            USTime += TIME_END - T3; //4
            // ACC
            Set<Long> except = session.run("match (p:Person)-[:KNOWS]->(f:Person)-[:LIKES]->(m:Comment) " +
                    "where p.face=$pface and f.face=$fface and m.target=$t\n" +
                    "return m.id order by m.creationDate desc limit 10",
                    Map.of("pface", parameters[0], "fface", parameters[1], "t", Integer.parseInt(parameters[2])))
                    .list().stream().map(record -> record.get(0).asLong()).collect(Collectors.toSet());
            long actualNum = result.stream().filter(record -> except.contains(record.get(5).asLong())).count();
            System.out.println("except: "+ except.size() + "act: "+ actualNum);
            acc += except.size()==0?1:((double) actualNum /except.size());
        }
        session.close();
        return new Result(allTime/taskTimes, STime/taskTimes, USTime/taskTimes, acc/taskTimes);
    }

    public  Result task3(List<String[]> parameterss){
        Session session = driver.session();
        int taskTimes = parameterss.size();
        long allTime = 0;
        long STime = 0;
        long USTime = 0;
        double acc = 0;

        for (String[] parameters : parameterss) {
            // 1. find friends
            long TIME_INIT = System.currentTimeMillis();
            List<Record> records = session.run("match (p:Person{id:"+parameters[0]+"})-[:KNOWS]->(f:Person)-[:IS_LOCATED_IN]->(c:City{name:'"+ parameters[2]+"'})  \n" +
                    "return f.id,f.face,f.firstName,f.lastName").list();
            long TIME_S_END = System.currentTimeMillis();
            // 2. filter by face
            List<Record> result = records.stream().filter(record ->
                            AIService.similarity(record.get(1).asString(), parameters[1]) >= IMAGE_SIMILAR)
                    .collect(Collectors.toList());
            long TIME_END = System.currentTimeMillis();
            allTime += TIME_END - TIME_INIT;
            STime += TIME_S_END - TIME_INIT;
            USTime += TIME_END - TIME_S_END;
            // ACC
            Set<Long> except = session.run("match (p:Person{id:"+parameters[0]+"})-[:KNOWS]->(f:Person{face:'"+parameters[1]+"'})-[:IS_LOCATED_IN]->(c:City{name:'"+ parameters[2]+"'})  \n"+
                                    " return f.id")
                    .list().stream().map(record -> record.get(0).asLong()).collect(Collectors.toSet());
            long actualNum = result.stream().filter(record -> except.contains(record.get(0).asLong())).count();
//            System.out.println("except: "+ except.size() + "act: "+ actualNum);
            acc += except.size()==0?1:((double) actualNum /except.size());
        }
        session.close();
        return new Result(allTime/taskTimes, STime/taskTimes, USTime/taskTimes, acc/taskTimes);
    }

    public  Result task4(){
        Session session = driver.session();
        int taskTimes = parameters4.size();
        long allTime = 0;
        long STime = 0;
        long USTime = 0;
        double acc = 0;

        for (String[] parameters : parameters4) {
            // return comment;
            long TIME_INIT = System.currentTimeMillis();
            List<Record> records = session.run("match (p:Person{id:"+parameters[0]+"})-[:KNOWS]->(f:Person)-[:LIKES]->(c:Comment)" +
                    "return f.id, c.id, c.content").list();
            System.out.println("comments: "+records.size());
            long T1 = System.currentTimeMillis();
            // filter by type
            Map<Long,Long> records1 = records.stream().filter(record ->
                            AIService.classifySenti(record.get(2).asString())==Integer.parseInt(parameters[1]))
                    .collect(Collectors.groupingBy(r->r.get(0).asLong(), Collectors.counting()));
            long T2 = System.currentTimeMillis();
//            System.out.println("filtered comments: "+records1.size());
//            // query other
//            long[] fids = records1.stream().mapToLong(record -> record.get(0).asLong()).toArray();
//            long[] cids = records1.stream().mapToLong(record -> record.get(1).asLong()).toArray();
//            System.out.println("fids: " + fids.length + "cids: "+ cids.length);
//            List<Record> result = session.run("match (f:Person)-[:LIKES]->(c:Comment) " +
//                    "where f.id in [$fids] and c.id in [$cids]" +
//                    "return f.id,f.firstName,f.lastName,count(c) order by f.id", Map.of("fids", fids, "cids", cids)).list();

            long TIME_END = System.currentTimeMillis();

            allTime += TIME_END - TIME_INIT;
            STime += T1 - TIME_INIT; //1
            USTime += T2 - T1; //2
            STime += TIME_END - T2; //3

            // ACC
            Map<Long,Long> except = session.run("match (p:Person{id:"+parameters[0]+"})-[:KNOWS]->(f:Person)-[:LIKES]->(c:Comment{target:"+ parameters[1] +"})" +
                                    "return f.id,f.firstName,f.lastName,count(c) order by f.id")
                    .list().stream().collect(Collectors.toMap(r -> r.get(0).asLong(), r-> r.get(3).asLong()));
//            int[] actual = records1.
//            int[] actual = result.stream().mapToInt(record -> record.get(3).asInt()).toArray();
//            System.out.println("except: "+ Arrays.toString(except) + "act: "+ Arrays.toString(actual));
            int right = 0;
            for (Long aLong : except.keySet()) {
                if (records1.containsKey(aLong) && records1.get(aLong)==except.get(aLong)) right++;
            }
            acc += (double) right /except.size();
            System.out.println(allTime +", " + STime +", "+ USTime+","+acc);
        }
        session.close();
        return new Result(allTime/taskTimes, STime/taskTimes, USTime/taskTimes, acc/taskTimes);
    }

    public  Result task5(){
        Session session = driver.session();
        int taskTimes = parameters5.size();
        long allTime = 0;
        long STime = 0;
        long USTime = 0;
        double acc = 0;

        for (String[] parameters : parameters5) {
            // 1. return persons;
            long TIME_INIT = System.currentTimeMillis();
            List<Record> records = session.run("match (p:Person)" +
                    "return p.id, p.face").list();
            long T1 = System.currentTimeMillis();

            // 2. filter by face
            List<Record> P = records.stream().filter(record ->
                            AIService.similarity(record.get(1).asString(), parameters[0]) >= IMAGE_SIMILAR )
                    .collect(Collectors.toList());
            long T2 = System.currentTimeMillis();

            // 3. match
            List<Record> comments = session.run("match (p:Person)-[:KNOWS*1..2]->(f:Person)<-[:HAS_CREATOR]-(m:Comment) " +
                            "where p.id in $pids " +
                            "return f.id,f.firstName,f.lastName,m.content,m.creationDate,m.id order by m.creationDate desc",
                    Map.of("pids", P.stream().mapToLong(record -> record.get(0).asLong()).toArray())).list();
            long T3 = System.currentTimeMillis();

            // 4. check type
            List<Record> result = comments.stream()
                    .filter(record -> AIService.classifySenti(record.get(3).asString())==Integer.parseInt(parameters[1]))
                    .limit(10)
                    .collect(Collectors.toList());
            long TIME_END = System.currentTimeMillis();

            allTime += TIME_END - TIME_INIT;
            STime += T1 - TIME_INIT; // 1
            USTime += T2 - T1; // 2
            STime += T3 - T2; // 3
            USTime += TIME_END - T3; //4
            // ACC
            Set<Long> except = session.run("match (p:Person)-[:KNOWS*1..2]->(f:Person)<-[:HAS_CREATOR]-(m:Comment) " +
                                    "where p.face=$pface and m.target=$t\n" +
                                    "return m.id order by m.creationDate desc limit 10",
                            Map.of("pface", parameters[0], "t", Integer.parseInt(parameters[1])))
                    .list().stream().map(record -> record.get(0).asLong()).collect(Collectors.toSet());
            long actualNum = result.stream().filter(record -> except.contains(record.get(5).asLong())).count();
            System.out.println("except: "+ except.size() + "act: "+ actualNum);
            acc += except.size()==0?1:((double) actualNum /except.size());
        }
        session.close();
        return new Result(allTime/taskTimes, STime/taskTimes, USTime/taskTimes, acc/taskTimes);
    }


    public  Result task6(List<String[]> parameterss){
        Session session = driver.session();
        int taskTimes = parameterss.size();
        long allTime = 0;
        long STime = 0;
        long USTime = 0;
        double acc = 0;

        for (String[] parameters : parameterss) {
            long TIME_INIT = System.currentTimeMillis();
            List<Record> records = session.run("match (p:Person{id:" + parameters[0] + "})<-[:HAS_CREATOR]-(post:Post)\n" +
                    "optional match (post)-[r:HAS_TOPIC]->(topic) \n" +
                    "return distinct post.id, post.content, topic.id, post.topic").list();
            long TIME_S_END = System.currentTimeMillis();
            List<Record> result = records.stream().map(record -> {
               if (!record.get(2).isNull()) return record;
               else {
                   Value[] newValue = record.values().toArray(new Value[0]);
                   newValue[2] = new IntegerValue(Objects.requireNonNull(AIService.extractTopic(List.of(newValue[1].asString()))).get(0));
                   return new InternalRecord(record.keys(), newValue);
               }
            }).collect(Collectors.toList());
            long TIME_END = System.currentTimeMillis();
            allTime += TIME_END - TIME_INIT;
            STime += TIME_S_END - TIME_INIT;
            USTime += TIME_END - TIME_S_END;
            // ACC
            long rightCount = result.stream().filter(record -> Integer.parseInt(record.get(3).asString()) == record.get(2).asInt()).count();
            acc += (double) rightCount /result.size();
        }
        session.close();
        return new Result(allTime/taskTimes, STime/taskTimes, USTime/taskTimes, acc/taskTimes);
    }

    public  Result task7(){
        Session session = driver.session();
        int taskTimes = parameters7.size();
        long allTime = 0;
        long STime = 0;
        long USTime = 0;
        double acc = 0;

        for (String[] parameters : parameters7) {
            // return persons;
            long TIME_INIT = System.currentTimeMillis();
            List<Record> records = session.run("match (p:Person)\n" +
                    "return p.id, p.face").list();
            long TIME_S_END = System.currentTimeMillis();
            // filter by face
            List<Record> records1 = records.stream().filter(record ->
                            AIService.similarity(record.get(1).asString(), parameters[1]) >= IMAGE_SIMILAR)
                    .collect(Collectors.toList());
            long TIME_US_END = System.currentTimeMillis();
            // query other
            long[] ids = records1.stream().mapToLong(record -> record.get(0).asLong()).toArray();
            List<Record> result = session.run("match pa=shortestpath((p:Person{id:"+parameters[0]+"})-[:KNOWS*]->(f:Person)) where f.id in $ids \n" +
                    "unwind nodes(pa) as n \n" +
                    "return n.id", Map.of("ids", ids)).list();
            long TIME_END = System.currentTimeMillis();
            allTime += TIME_END - TIME_INIT;
            STime += TIME_S_END - TIME_INIT;
            STime += TIME_END - TIME_US_END;
            USTime += TIME_US_END - TIME_S_END;
            // ACC
            Set<Long> except = session.run("match pa=shortestpath((p:Person{id:"+parameters[0]+"})-[:KNOWS*]->(f:Person{face:'"+parameters[1]+"'})) " +
                                    "unwind nodes(pa) as n \n" +
                                    "return n.id")
                    .list().stream().map(record -> record.get(0).asLong()).collect(Collectors.toSet());
            long actualNum = result.stream().filter(record -> except.contains(record.get(0).asLong())).count();
            System.out.println("except: "+ except.size() + "act: "+ actualNum);
            acc += except.size()==0?1:((double) actualNum /except.size());
        }
        session.close();
        return new Result(allTime/taskTimes, STime/taskTimes, USTime/taskTimes, acc/taskTimes);
    }

    public  Result task8(){
        Session session = driver.session();
        int taskTimes = parameters8.size();
        long allTime = 0;
        long STime = 0;
        long USTime = 0;
        double acc = 0;

        for (String[] parameters : parameters8) {
            // return persons;
            long TIME_INIT = System.currentTimeMillis();
            List<Record> records = session.run("match (p:Person)\n" +
                    "return p.id, p.face").list();
            long TIME_S_END = System.currentTimeMillis();
            // filter by face
            List<Record> records1 = records.stream().filter(record ->
                            AIService.similarity(record.get(1).asString(), parameters[0]) >= IMAGE_SIMILAR)
                    .collect(Collectors.toList());
            long TIME_US_END = System.currentTimeMillis();
            // query other
            long[] ids = records1.stream().mapToLong(record -> record.get(0).asLong()).toArray();
            session.run("match (p:Person)-[:IS_LOCATED_IN]->(c:City) where p.id in $ids \n" +
                    "return p.id,p.firstName,p.lastName,p.birthday,p.locationIP,p.browserUsed,\n" +
                    "p.creationDate,c.name", Map.of("ids", ids));
            long TIME_END = System.currentTimeMillis();
            allTime += TIME_END - TIME_INIT;
            STime += TIME_S_END - TIME_INIT;
            STime += TIME_END - TIME_US_END;
            USTime += TIME_US_END - TIME_S_END;
            long rightNum = records.stream().filter(record -> record.get(1).asString().equals(parameters[0])).count();
            long actualNum = records1.stream().filter(record -> record.get(1).asString().equals(parameters[0])).count();
            acc += (actualNum==rightNum?1:0);
        }
        session.close();
        return new Result(allTime/taskTimes, STime/taskTimes, USTime/taskTimes, acc/taskTimes);
    }

    public  Result task9(){
        Session session = driver.session();
        int taskTimes = parameters9.size();
        long allTime = 0;
        long STime = 0;
        long USTime = 0;
        double acc = 0;

        for (String[] parameters : parameters9) {
            // return persons;
            long TIME_INIT = System.currentTimeMillis();
            List<Record> records = session.run("match (p:Person)\n" +
                    "return p.id, p.face").list();
            long TIME_S_END = System.currentTimeMillis();
            // filter by face
            List<Record> records1 = records.stream().filter(record ->
                            AIService.similarity(record.get(1).asString(), parameters[0]) >= IMAGE_SIMILAR)
                    .collect(Collectors.toList());
            long TIME_US_END = System.currentTimeMillis();
            long[] ids = records1.stream().mapToLong(record -> record.get(0).asLong()).toArray();
            List<Record> result = session.run("match (p:Person)-[r:KNOWS]->(f:Person) where p.id in $ids \n" +
                    "return f.face, f.id, f.firstName, f.lastName, r.creationDate", Map.of("ids", ids)).list();
            long TIME_END = System.currentTimeMillis();
            allTime += TIME_END - TIME_INIT;
            STime += TIME_S_END - TIME_INIT;
            STime += TIME_END - TIME_US_END;
            USTime += TIME_US_END - TIME_S_END;

            long rightNum = records.stream().filter(record -> record.get(1).asString().equals(parameters[0])).count();
            long actualNum = result.stream().filter(record -> record.get(0).asString().equals(parameters[0])).count();
            acc += (actualNum==rightNum?1:0);
        }
        session.close();
        return new Result(allTime/taskTimes, STime/taskTimes, USTime/taskTimes, acc/taskTimes);
    }

    public  Result task10(List<String[]> parameterss){
        Session session = driver.session();
        int taskTimes = parameterss.size();
        long allTime = 0;
        long STime = 0;
        long USTime = 0;
        double acc = 0;

        for (String[] parameters : parameterss) {
            long TIME_INIT = System.currentTimeMillis();
            List<Record> records = session.run("match (c:Comment{id:" + parameters[0] + "})\n" +
                    "return c.content,c.target").list();
            long TIME_S_END = System.currentTimeMillis();
            int[] result = records.stream().mapToInt(r -> AIService.classifySenti(r.get(0).asString())).toArray();
            long TIME_END = System.currentTimeMillis();
            allTime += TIME_END - TIME_INIT;
            STime += TIME_S_END - TIME_INIT;
            USTime += TIME_END - TIME_S_END;
            acc += acc(records.stream().mapToInt(record -> record.get(1).asInt()).toArray(), result);
        }
        session.close();
        return new Result(allTime/taskTimes, STime/taskTimes, USTime/taskTimes, acc/taskTimes);
    }

    /**
     * give a person with id, return  sentiment distribution of last 10 message he or she sends.
     */
    public  Result task11(List<String[]> parameterss){
        Session session = driver.session();
        int taskTimes = parameterss.size();
        long allTime = 0;
        long STime = 0;
        long USTime = 0;
        double acc = 0;

        for (String[] parameters : parameterss) {
            long TIME_INIT = System.currentTimeMillis();
            List<Record> records = session.run("match (p:Person{id:" + parameters[0] + "})<-[:HAS_CREATOR]-(c:Comment)  \n" +
                    "return c.content,c.target order by c.creationDate desc limit 10").list();
            long TIME_S_END = System.currentTimeMillis();
            int[] result = records.stream().mapToInt(r -> AIService.classifySenti(r.get(0).asString())).toArray();
//            System.out.println(Arrays.toString(result));
            long TIME_END = System.currentTimeMillis();
            allTime += TIME_END - TIME_INIT;
            STime += TIME_S_END - TIME_INIT;
            USTime += TIME_END - TIME_S_END;
            acc += acc(records.stream().mapToInt(record -> record.get(1).asInt()).toArray(), result);
        }
        session.close();
        return new Result(allTime/taskTimes, STime/taskTimes, USTime/taskTimes, acc/taskTimes);
    }

    public  double acc(int[] except, int[] actual) {
        if (except.length != actual.length) return  0;
        int acc = 0;
        for (int i = 0; i < except.length; i++) {
            if (except[i]==actual[i]) acc++;
        }
        return (double) acc /except.length;
    }

    public  void run(int taskNum, Result result){
        System.out.println("========Result of Task "+taskNum+"========");
        System.out.println("Avg Time: "+ result.time+"ms");
        System.out.println("Avg Structured Time: "+ result.structuredTime+"ms");
        System.out.println("Avg Unstructured Time: "+ result.unstructuredTime+"ms");
        System.out.println("Avg Acc: "+ result.acc);
    }

    public  List<String[]> parameters1 = Arrays.asList( //<personId>, firstname, face
            new String[]{"9034","Angela", "/imdb_crop/28/nm0000128_rm1090620160_1964-4-7_2010.jpg"},
            new String[]{"2199023263285","Daisuke" ,"/imdb_crop/28/nm0000128_rm2243660288_1964-4-7_2008.jpg"},
            new String[]{"6597069771139","Ahmad Rafiq","/imdb_crop/28/nm0000128_rm1921092608_1964-4-7_2003.jpg"},
            new String[]{"10995116279686","Sam", "/imdb_crop/25/nm0000125_rm2062716160_1930-8-25_1962.jpg"},
            new String[]{"13194139543225","Jason", "/imdb_crop/28/nm0000128_rm1388484864_1964-4-7_2006.jpg"},
            new String[]{"17592186050850","Lei", "/imdb_crop/26/nm0000126_rm647404288_1955-1-18_2005.jpg"},
            new String[]{"21990232559455","Alejandro", "/imdb_crop/26/nm0000126_rm1803932672_1955-1-18_2014.jpg"},
            new String[]{"26388279067812","Sergio", "/imdb_crop/45/nm0000045_rm1933154304_1940-11-27_1972.jpg"},
            new String[]{"28587302331285","Deepak", "/imdb_crop/28/nm0000128_rm2663773696_1964-4-7_2014.jpg"},
            new String[]{"32985348839582","Zhong", "/imdb_crop/25/nm0000125_rm589333760_1930-8-25_1978.jpg"});



    public  final List<String[]> parameters2 = Arrays.asList( //<face>, <face>, type
//            new String[]{"/imdb_crop/62/nm0000062_rm3777212416_1935-1-8_1981.jpg", "/imdb_crop/28/nm0000128_rm1090620160_1964-4-7_2010.jpg","4"},
//            new String[]{"/imdb_crop/86/nm0000086_rm4141414144_1914-7-31_1964.jpg", "/imdb_crop/28/nm0000128_rm2243660288_1964-4-7_2008.jpg","4"},
//            new String[]{"/imdb_crop/95/nm0000095_rm127633152_1935-12-1_1979.jpg", "/imdb_crop/28/nm0000128_rm2243660288_1964-4-7_2008.jpg","4"},
//            new String[]{"/imdb_crop/98/nm0000098_rm3774602496_1969-2-11_1994.jpg", "/imdb_crop/25/nm0000125_rm2062716160_1930-8-25_1962.jpg","4"},
//            new String[]{"/imdb_crop/02/nm0000102_rm3460481792_1958-7-8_2013.jpg", "/imdb_crop/28/nm0000128_rm1388484864_1964-4-7_2006.jpg","4"},
//            new String[]{"/imdb_crop/06/nm0000106_rm796118528_1975-2-22_2014.jpg", "/imdb_crop/26/nm0000126_rm647404288_1955-1-18_2005.jpg","4"},
//            new String[]{"/imdb_crop/12/nm0000112_rm842177024_1953-5-16_1999.jpg", "/imdb_crop/26/nm0000126_rm1803932672_1955-1-18_2014.jpg","0"},
//            new String[]{"/imdb_crop/15/nm0000115_rm279544832_1964-1-7_2011.jpg", "/imdb_crop/45/nm0000045_rm1933154304_1940-11-27_1972.jpg","0"},
            new String[]{"/imdb_crop/20/nm0000120_rm4265577984_1962-1-17_1996.jpg", "/imdb_crop/28/nm0000128_rm2663773696_1964-4-7_2014.jpg","0"},
            new String[]{"/imdb_crop/25/nm0000125_rm1100336640_1930-8-25_1989.jpg", "/imdb_crop/25/nm0000125_rm589333760_1930-8-25_1978.jpg","0"});

    public  final List<String[]> parameters3 = Arrays.asList( //<id>, <face>, city
            new String[]{"9034", "/imdb_crop/28/nm0000128_rm1090620160_1964-4-7_2010.jpg", "Monagas"},
            new String[]{"2199023263285", "/imdb_crop/28/nm0000128_rm2243660288_1964-4-7_2008.jpg", "Yokohama"},
            new String[]{"6597069771139", "/imdb_crop/28/nm0000128_rm2243660288_1964-4-7_2008.jpg", "Depok"},
            new String[]{"10995116279686", "/imdb_crop/25/nm0000125_rm2062716160_1930-8-25_1962.jpg", "Taipei"},
            new String[]{"13194139543225", "/imdb_crop/28/nm0000128_rm1388484864_1964-4-7_2006.jpg", "Winnipeg"},
            new String[]{"17592186050850", "/imdb_crop/26/nm0000126_rm647404288_1955-1-18_2005.jpg", "Datong"},
            new String[]{"21990232559455", "/imdb_crop/26/nm0000126_rm1803932672_1955-1-18_2014.jpg", "Puerto_Montt"},
            new String[]{"26388279067812", "/imdb_crop/45/nm0000045_rm1933154304_1940-11-27_1972.jpg", "Belo_Horizonte"},
            new String[]{"28587302331285", "/imdb_crop/28/nm0000128_rm2663773696_1964-4-7_2014.jpg", "Kollam"},
            new String[]{"32985348839582", "/imdb_crop/25/nm0000125_rm589333760_1930-8-25_1978.jpg", "Gaocheng"});

    public  final List<String[]> parameters4 = Arrays.asList( //<personid>, type
            new String[]{"9034", "4"},
            new String[]{"2199023263285", "4"},
            new String[]{"6597069771139", "4"},
            new String[]{"10995116279686", "4"},
            new String[]{"13194139543225", "4"},
            new String[]{"17592186050850", "4"},
            new String[]{"21990232559455", "4"},
            new String[]{"26388279067812", "4"},
            new String[]{"28587302331285", "4"},
            new String[]{"32985348839582", "4"});


    public  final List<String[]> parameters5 = Arrays.asList( //<face> type
//            new String[]{"/imdb_crop/72/nm0000072_rm2223557632_1932-2-27_1963.jpg", "0"},
//            new String[]{"/imdb_crop/92/nm0000092_rm3459894272_1939-10-27_1975.jpg", "0"},
//            new String[]{"/imdb_crop/95/nm0000095_rm127633152_1935-12-1_1979.jpg", "0"},
//            new String[]{"/imdb_crop/98/nm0000098_rm3774602496_1969-2-11_1994.jpg", "0"},
//            new String[]{"/imdb_crop/02/nm0000102_rm3460481792_1958-7-8_2013.jpg", "0"},
//            new String[]{"/imdb_crop/06/nm0000106_rm796118528_1975-2-22_2014.jpg", "4"},
//            new String[]{"/imdb_crop/12/nm0000112_rm842177024_1953-5-16_1999.jpg", "4"},
//            new String[]{"/imdb_crop/15/nm0000115_rm3542399744_1964-1-7_2002.jpg", "4"},
            new String[]{"/imdb_crop/23/nm0000123_rm3797801216_1961-5-6_2013.jpg", "4"},
            new String[]{"/imdb_crop/25/nm0000125_rm1100336640_1930-8-25_1989.jpg", "4"});


    public  final List<String[]> parameters6 = Arrays.asList( //<personid>
            new String[]{"9034"},
            new String[]{"2199023263285"},
            new String[]{"6597069771139"},
            new String[]{"10995116279686"},
            new String[]{"13194139543225"},
            new String[]{"17592186050850"},
            new String[]{"21990232559455"},
            new String[]{"26388279067812"},
            new String[]{"28587302331285"},
            new String[]{"32985348839582"});
    public  final List<String[]> parameters7 = Arrays.asList( //<face>
//            new String[]{"9034", "/imdb_crop/28/nm0000128_rm1090620160_1964-4-7_2010.jpg"},
//            new String[]{"2199023263285", "/imdb_crop/28/nm0000128_rm2243660288_1964-4-7_2008.jpg"},
//            new String[]{"6597069771139", "/imdb_crop/28/nm0000128_rm2243660288_1964-4-7_2008.jpg"},
//            new String[]{"10995116279686", "/imdb_crop/25/nm0000125_rm2062716160_1930-8-25_1962.jpg"},
//            new String[]{"13194139543225", "/imdb_crop/28/nm0000128_rm1388484864_1964-4-7_2006.jpg"},
//            new String[]{"17592186050850", "/imdb_crop/26/nm0000126_rm647404288_1955-1-18_2005.jpg"},
//            new String[]{"21990232559455", "/imdb_crop/26/nm0000126_rm1803932672_1955-1-18_2014.jpg"},
//            new String[]{"26388279067812", "/imdb_crop/45/nm0000045_rm1933154304_1940-11-27_1972.jpg"},
            new String[]{"28587302331285", "/imdb_crop/28/nm0000128_rm2663773696_1964-4-7_2014.jpg"},
            new String[]{"32985348839582", "/imdb_crop/25/nm0000125_rm589333760_1930-8-25_1978.jpg"});

    public  final List<String[]> parameters8 = Arrays.asList( //<face>
            new String[]{"/imdb_crop/28/nm0000128_rm1090620160_1964-4-7_2010.jpg"},
            new String[]{"/imdb_crop/28/nm0000128_rm2243660288_1964-4-7_2008.jpg"},
            new String[]{"/imdb_crop/28/nm0000128_rm2243660288_1964-4-7_2008.jpg"},
            new String[]{"/imdb_crop/25/nm0000125_rm2062716160_1930-8-25_1962.jpg"},
            new String[]{"/imdb_crop/28/nm0000128_rm1388484864_1964-4-7_2006.jpg"},
            new String[]{"/imdb_crop/26/nm0000126_rm647404288_1955-1-18_2005.jpg"},
            new String[]{"/imdb_crop/26/nm0000126_rm1803932672_1955-1-18_2014.jpg"},
            new String[]{"/imdb_crop/45/nm0000045_rm1933154304_1940-11-27_1972.jpg"},
            new String[]{"/imdb_crop/28/nm0000128_rm2663773696_1964-4-7_2014.jpg"},
            new String[]{"/imdb_crop/25/nm0000125_rm589333760_1930-8-25_1978.jpg"});

    public  final List<String[]> parameters9 = Arrays.asList( //<id>, <face>,
            new String[]{"/imdb_crop/28/nm0000128_rm1090620160_1964-4-7_2010.jpg"},
            new String[]{"/imdb_crop/28/nm0000128_rm2243660288_1964-4-7_2008.jpg"},
            new String[]{"/imdb_crop/28/nm0000128_rm2243660288_1964-4-7_2008.jpg"},
            new String[]{"/imdb_crop/25/nm0000125_rm2062716160_1930-8-25_1962.jpg"},
            new String[]{"/imdb_crop/28/nm0000128_rm1388484864_1964-4-7_2006.jpg"},
            new String[]{"/imdb_crop/26/nm0000126_rm647404288_1955-1-18_2005.jpg"},
            new String[]{"/imdb_crop/26/nm0000126_rm1803932672_1955-1-18_2014.jpg"},
            new String[]{"/imdb_crop/45/nm0000045_rm1933154304_1940-11-27_1972.jpg"},
            new String[]{"/imdb_crop/28/nm0000128_rm2663773696_1964-4-7_2014.jpg"},
            new String[]{"/imdb_crop/25/nm0000125_rm589333760_1930-8-25_1978.jpg"});

    public  final List<String[]> parameters10 = Arrays.asList( //<commentId>
            new String[]{"1236953724222"},
            new String[]{"1511829120506"},
            new String[]{"1786706757910"},
            new String[]{"1924146103281"},
            new String[]{"1924151756279"},
            new String[]{"2061587247322"},
            new String[]{"2199023649884"},
            new String[]{"2199025818384"},
            new String[]{"2199028071140"},
            new String[]{"2336463314268"});
    public final List<String[]> parameters11 = Arrays.asList( //<personId>
            new String[]{"9034"},
            new String[]{"2199023263285"},
            new String[]{"6597069771139"},
            new String[]{"10995116279686"},
            new String[]{"13194139543225"},
            new String[]{"17592186050850"},
            new String[]{"21990232559455"},
            new String[]{"26388279067812"},
            new String[]{"28587302331285"},
            new String[]{"32985348839582"});

    public static void main(String[] args) {
         String uri = "bolt://10.0.82.144:9687";
         String user = "neo4j";
         String pwd = "neo4j";
          Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, pwd));
        Tasks tasks = new Tasks(driver);
//        run(1, task1());
//        tasks.run(2, tasks.task2());
        tasks.run(3, tasks.task3(tasks.parameters3));
//        tasks.run(10, tasks.task10(tasks.parameters10));
//        tasks.run(11, tasks.task11(tasks.parameters11));
//        run(4, task4());
//        tasks.run(5, tasks.task5());
//        tasks.run(7, tasks.task7());
//        run(6, task6());
//        run(7, task7());
//        run(8, task8()); // slow
//        run(9, task9());
//        run(10, task10());
//        run(11, task11());
//        long t0 = System.currentTimeMillis();
//        for (int i = 0; i < 100; i++) {
//            AIService.similarity("/imdb_crop/25/nm0000125_rm589333760_1930-8-25_1978.jpg",
//                    "/imdb_crop/25/nm0000125_rm589333760_1930-8-25_1978.jpg");
//        }
//        System.out.println(System.currentTimeMillis() - t0);
    }
}
