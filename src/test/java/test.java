import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
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

}
