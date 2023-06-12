import com.google.gson.JsonObject;

import java.io.*;
import java.util.Arrays;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/5/15 17:01
 * @Version 1.0
 */
public class Test {
    public static void main(String[] args) throws IOException {
//        String s = "metaPhoneDf,reviewPhoneDf,metaClothDf,reviewClothDf,metaFoodDf,reviewFoodDf,metaHomeDf,reviewHomeDf,metaMusicalDf,reviewMusicalDf,metaToolsDf,reviewToolsDf";
////        Arrays.asList(s.split(",")).stream().filter(e->e.contains("meta")).map(e->String.format("%s = %s.filter(size(col(\"imageURLHighRes\")).notEqual(0).and(col(\"price\").notEqual(\"\")).and(col(\"brand\").notEqual(\"\")));",e,e)).forEach(System.out::println);
////        Arrays.asList(s.split(",")).stream().map(e->String.format("%s.show();",e)).forEach(System.out::println);
//
//        Arrays.asList(s.split(",")).stream().filter(e->e.contains("review")).map(e->String.format("%s.count();",e)).forEach(System.out::println);
        BufferedReader reader = new BufferedReader(new FileReader("/Users/along/Documents/dataset/TweetDataset/training.1600000.processed.noemoticon.csv"));

        String line;
        PrintWriter printWriter = new PrintWriter(new File("/Users/along/Documents/dataset/TweetDataset/sentiment160.json"));
        while ((line = reader.readLine()) != null) {
            String[] parts = split(line);
            JsonObject object = new JsonObject();
            try {
                object.addProperty("target", parts[0].replace("\"", ""));
                object.addProperty("ids", parts[1].replace("\"", ""));
                object.addProperty("date", parts[2].replace("\"", ""));
                object.addProperty("user", parts[4].replace("\"", ""));
                object.addProperty("text", parts[5].replace("\"", ""));

            } catch (Exception e) {
                System.out.println(line);
                e.printStackTrace();
                throw new RuntimeException();
            }
            printWriter.println(object);
        }
        printWriter.close();

    }

    public static String[] split(String line) {
        String[] parts = new String[6];
        int count = 0;
        for (int i = 0, j = 0; j < line.length(); j++) {
            if (line.charAt(j) == ',') {
                parts[count++] = line.substring(i, j);
                i = j + 1;
                if (count == 5) {
                    parts[count] = line.substring(j + 1);
                    break;
                }
            }
        }
        return parts;
    }
}
