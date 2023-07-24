import entity.Face;
import entity.News;
import entity.SentimentText;
import enums.DatasetEnum;
import factory.DataSelectStrategyFactory;
import org.apache.log4j.Logger;
import strategy.DataSelectStrategy;

import java.io.*;
import java.util.*;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/11 20:27
 * @Version 1.0
 */
public class Gen {

    Logger logger = Logger.getLogger(Gen.class);

    private final String personPath = "/Users/along/github/ldbc_snb_datagen_spark/out-sf0.003/graphs/csv/bi/composite-projected-fk/initial_snapshot/dynamic/Person";

    private final String commentPath = "/Users/along/github/ldbc_snb_datagen_spark/out-sf0.003/graphs/csv/bi/composite-projected-fk/initial_snapshot/dynamic/Comment";

    private final String postPath = "/Users/along/github/ldbc_snb_datagen_spark/out-sf0.003/graphs/csv/bi/composite-projected-fk/initial_snapshot/dynamic/Post";

    private final String postHasTopicPath = "/Users/along/github/ldbc_snb_datagen_spark/out-sf0.003/graphs/csv/bi/composite-projected-fk/initial_snapshot/dynamic/Post_Has_Topic";

    private final String topicPath = "/Users/along/github/ldbc_snb_datagen_spark/out-sf0.003/graphs/csv/bi/composite-projected-fk/initial_snapshot/dynamic/Topic";

    private final String topicDictFileName = "topic.txt";

    public void merge() {
        try {
            mergeFace();
            mergeContent();
            mergePost();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 为每一个Person生成一张人脸
     * @throws IOException
     */
    private void mergeFace() throws IOException {
        File targetFile = getTargetFile(personPath);
        if (null != targetFile) {
            DataSelectStrategy dataSelectStrategy = DataSelectStrategyFactory.create(DatasetEnum.FACE);
            BufferedReader reader = new BufferedReader(new FileReader(targetFile.getAbsolutePath()));
            String line;
            PrintWriter out = new PrintWriter(personPath + "/part-new.csv");
            while ((line = reader.readLine()) != null) {
                String faceImagePath = ((Face) dataSelectStrategy.select()).getFacePath();
                out.println(line + "|" + faceImagePath);
            }
            out.close();
            reader.close();
        } else {
            logger.error("Person csv file not found");
        }
    }

    /**
     * 覆盖原来的comment文本
     * @throws IOException
     */
    private void mergeContent() throws IOException{
        File targetFile = getTargetFile(commentPath);
        if (null != targetFile) {
            DataSelectStrategy dataSelectStrategy = DataSelectStrategyFactory.create(DatasetEnum.SENTIMENT_TEXT);
            BufferedReader reader = new BufferedReader(new FileReader(targetFile.getAbsolutePath()));
            String line;
            PrintWriter out = new PrintWriter(commentPath + "/part-new.csv");
            while ((line = reader.readLine()) != null){
                SentimentText sentimentText = dataSelectStrategy.select();
                String[] fields = line.split("\\|");
                String newLine = "";
                for(int i=0;i<fields.length-2;i++){
                    newLine+=fields[i]+"|";
                }
                out.println(String.format("%s\"%s\"|\"%d\"|\"%s\"",newLine,sentimentText.getText(),sentimentText.getText().length(),sentimentText.getTarget()));
            }
            out.close();
            reader.close();
        }else{
            logger.error("Comment csv file not found");
        }
    }

    private void mergePost() throws Exception{
        File targetFile = getTargetFile(postPath);
        if (null != targetFile) {
            DataSelectStrategy dataSelectStrategy = DataSelectStrategyFactory.create(DatasetEnum.NEWS);
            Map<String,String> postHasTopicMap = new HashMap<>();
            try(
                    BufferedReader reader = new BufferedReader(new FileReader(targetFile.getAbsolutePath()));
                    BufferedWriter out = new BufferedWriter(new FileWriter(postPath + "/part-new.csv"));
            ) {
                String line;
                while ((line = reader.readLine()) != null) {
                    News news = dataSelectStrategy.select();
                    String[] fields = line.split("\\|");
                    String newLine = "";
                    //修改三个字段 language:STRING|content:STRING|length:LONG
                    String originPostId = fields[1];
                    for (int i = 0; i < fields.length - 3; i++) {
                        newLine += fields[i] + "|";
                    }
                    out.write(String.format("%s\"en\"|\"%s\"|\"%d\"|\"%s\"\n", newLine, news.getDescription(), news.getDescription().length(), news.getTopic()));
                    postHasTopicMap.put(originPostId,news.getTopic());
                }
            }
            createTopic();
            createPostHasTopic(postHasTopicMap);
        }else{
            logger.error("Post csv file not found");
        }
    }

    /**
     *
     * @throws Exception
     */
    private void createTopic() throws Exception{
        File topic = new File(topicPath);
        if(!topic.exists()){
            topic.createNewFile();
        }
        try(
                BufferedWriter bw = new BufferedWriter(new FileWriter(topic));
                BufferedReader br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(topicDictFileName),"UTF-8"));
        ){
            String line;
            while((line=br.readLine())!=null){
                String labelNum = line.split("\t")[0];
                String labelDesc = line.split("\t")[1];
                bw.write(String.format("\"%s\"|\"%s\"\n",labelNum,labelDesc));
            }
            logger.info("createTopic finish!");
        }
    }


    /**
     *
     * @param postHasTopicMap
     */
    private void createPostHasTopic(Map<String,String> postHasTopicMap){
        try(
                BufferedWriter bw = new BufferedWriter(new FileWriter(postHasTopicPath));
        ){
            List<Map.Entry<String,String>> entries = new ArrayList<>(postHasTopicMap.entrySet());
            Collections.shuffle(entries,new Random(42));

            int keepSize = (int)(entries.size()*0.6);
            for(int i=0;i<keepSize;i++){
                bw.write(String.format(String.format("\"%s\"|\"%s\"\n",entries.get(i).getKey(), entries.get(i).getValue())));
            }
            logger.info("create postHasTopic finish!");
        } catch (IOException e) {
            logger.error("write postHasTopic file error",e);
        }
    }


    /**
     * 获取ldbc csv文件
     *
     * @param parentDir
     * @return
     */
    private File getTargetFile(String parentDir) {
        File[] files = new File(parentDir).listFiles();
        File targetFile = null;
        for (File file : files) {
            if (file.getName().startsWith("part")&&file.getName().endsWith("csv")) {
                targetFile = file;
                break;
            }
        }
        return targetFile;
    }

    public static void main(String[] args) {
        Gen main = new Gen();
        main.merge();
    }
}
