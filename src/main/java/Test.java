import java.util.Arrays;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/5/15 17:01
 * @Version 1.0
 */
public class Test {
    public static void main(String[] args) {
        String s = "metaPhoneDf,reviewPhoneDf,metaClothDf,reviewClothDf,metaFoodDf,reviewFoodDf,metaHomeDf,reviewHomeDf,metaMusicalDf,reviewMusicalDf,metaToolsDf,reviewToolsDf";
//        Arrays.asList(s.split(",")).stream().filter(e->e.contains("meta")).map(e->String.format("%s = %s.filter(size(col(\"imageURLHighRes\")).notEqual(0).and(col(\"price\").notEqual(\"\")).and(col(\"brand\").notEqual(\"\")));",e,e)).forEach(System.out::println);
//        Arrays.asList(s.split(",")).stream().map(e->String.format("%s.show();",e)).forEach(System.out::println);

        Arrays.asList(s.split(",")).stream().filter(e->e.contains("review")).map(e->String.format("%s.count();",e)).forEach(System.out::println);

    }
}
