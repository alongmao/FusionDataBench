import task.fusionDB.AIService;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/7/20 22:48
 * @Version 1.0
 */
public class test {
    private static final String dir = "/Users/along/Documents/dataset/MIND";
    static HashMap<String, Integer> hashMap = new HashMap<>();

    public static void solve(String name) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(String.format("%s/%s.csv",dir,name)));
        PrintWriter out = new PrintWriter(String.format("%s/%s.txt",dir,name));

        while (scanner.hasNext()){
            String[] lines = scanner.nextLine().split("\t");
            if(lines.length<2||lines[0].trim().equals("")||!hashMap.containsKey(lines[1])||hashMap.get(lines[1])<1000){
                continue;
            }
            out.write(String.format("%s\t%s\n",lines[0],lines[1]));
        }
        out.close();
    }

    public static void main(String[] args) throws FileNotFoundException {

        cal("train");
        cal("test");
        cal("dev");


        for(String key:hashMap.keySet()){
            System.out.println(String.format("key:%s\tcount:%d",key,hashMap.get(key)));
        }

        int i=0;
        for(String key:hashMap.keySet()){
            System.out.println(hashMap.put(key,i++));
        }

        change("train");
        change("dev");
        change("test");

//        solve("train");
//        solve("dev");
//        solve("test");
    }

    public static void cal(String name) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(String.format("%s/%s.txt", dir, name)));
        while(scanner.hasNext()){
            String[] lines = scanner.nextLine().split("\t");
            if(hashMap.containsKey(lines[1])){
                hashMap.put(lines[1],hashMap.get(lines[1])+1);
            }else{
                hashMap.put(lines[1],1);
            }
        }
    }

    public static void change(String name) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(String.format("%s/%s.txt", dir, name)));
        PrintWriter out = new PrintWriter(String.format("%s/%s_1.txt", dir, name));
        while(scanner.hasNext()){
            String[] lines = scanner.nextLine().split("\t");
            out.write(String.format("%s\t%s\n",lines[0],hashMap.get(lines[1])));
        }
        out.close();
    }

    public static void testTopicExtract(){
        AIService.extractTopic(List.of(
                "Over 4 Million Americans Roll Up Sleeves For Omicron-Targeted COVID Boosters Health experts said it is too early to predict whether demand would match up with the 171 million doses of the new boosters the U.S. ordered for the fall.",
                "American Airlines Flyer Charged, Banned For Life After Punching Flight Attendant On Video He was subdued by passengers and crew when he fled to the back of the aircraft after the confrontation, according to the U.S. attorney's office in Los Angeles.",
                "23 Of The Funniest Tweets About Cats And Dogs This Week (Sept. 17-23) Until you have a dog you don't understand what could be eaten."
        )).forEach(System.out::println);
        System.out.println(AIService.classifySenti("I am so happy"));
    }

}
