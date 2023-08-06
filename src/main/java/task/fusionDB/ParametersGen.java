package task.fusionDB;

import org.neo4j.driver.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ParametersGen {
    private static final String uri = "bolt://10.0.82.144:9687";
    private static final String user = "neo4j";
    private static final String pwd = "neo4j";
    private static final Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, pwd));

    private static long[] personIdGen() {
        Session session = driver.session();
        long[] result = List.of(0.06, 0.11, 0.2, 0.3, 0.4, 0.5, 0.6, 0.71, 0.85, 0.9).stream().mapToLong(aDouble ->
                session.run("match(p:Person) return percentileDisc(p.id,"+ aDouble +")").single().get(0).asLong()).toArray();
        for (long l : result) {
            System.out.println(l);
        }
        session.close();
        return result;
    }

    private static long[] commentIdGen() {
        Session session = driver.session();
        long[] result = List.of(0.06, 0.11, 0.2, 0.3, 0.4, 0.5, 0.6, 0.71, 0.85, 0.91).stream().mapToLong(aDouble ->
                session.run("match(p:Comment) return percentileDisc(p.id,"+ aDouble +")").single().get(0).asLong()).toArray();
        System.out.println(Arrays.toString(result));
        session.close();
        return result;
    }

    // personId, personFace, friendId, friendFace, City
    private static String[] generate(long personId){
        Session session = driver.session();
        String[] result = session.run("match(p:Person{id:" + personId + "})-[:KNOWS]->(f)-[:IS_LOCATED_IN]->(c) " +
                "return toString(p.id), p.face, toString(f.id),f.firstName, f.face, c.name limit 1").single().values().stream().map(Value::toString).toArray(String[]::new);
        session.close();
//        System.out.println(Arrays.toString(result));
        return result;
    }

    public static void main(String[] args) {
        long[] ids = personIdGen();
        for (long id : ids) {
            String[] a = generate(id);
            System.out.println(Arrays.toString(a));
        }
//        commentIdGen();
    }
}
