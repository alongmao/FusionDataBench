import entity.Face;
import entity.News;
import entity.SentimentText;
import enums.DatasetEnum;
import factory.DataSelectStrategyFactory;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
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

    transient Logger logger = Logger.getLogger(Gen.class);

    private final String ldbcDataPath = "/Users/along/github/ldbc_snb_datagen_spark/out-sf1/graphs/csv/bi/composite-projected-fk";

    private final String personPath = ldbcDataPath + "/initial_snapshot/dynamic/Person";

    private final String commentPath = ldbcDataPath + "/initial_snapshot/dynamic/Comment";

    private final String postPath = ldbcDataPath + "/initial_snapshot/dynamic/Post";

    private final String postHasTopicPath = ldbcDataPath + "/initial_snapshot/dynamic/Post_HasTopic_Topic";

    private final String topicPath = ldbcDataPath + "/initial_snapshot/static/Topic";

    private final String topicDictFileName = "topic.txt";

    public void merge() {
        SparkConf conf = new SparkConf()
                .setAppName("DataProcessor")
                .setMaster("local[*]"); // Change "local[*]" to the Spark master URL in a distributed environment
        JavaSparkContext sc = new JavaSparkContext(conf);
        try {
            mergeFace(sc);
            mergeContent(sc);
            mergePost(sc);
//            replaceHeader();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 为每一个Person生成一张人脸
     *
     * @throws IOException
     */
    private void mergeFace(JavaSparkContext sc) throws IOException {
        List<File> targetFileList = getTargetFile(personPath);
        if (null != targetFileList && targetFileList.size() > 0) {
            DataSelectStrategy dataSelectStrategy = DataSelectStrategyFactory.create(DatasetEnum.FACE, sc);
            targetFileList.forEach(targetFile -> {
                try (
                        PrintWriter out = new PrintWriter(personPath + "/" + targetFile.getName().replace(".csv", "-new.csv"));
                        BufferedReader reader = new BufferedReader(new FileReader(targetFile.getAbsolutePath()))
                ) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String faceImagePath = ((Face) dataSelectStrategy.select()).getFacePath();
                        out.println(line + "|" + faceImagePath);
                    }
                } catch (Exception e) {
                    logger.error("process person file error", e);
                }

            });
            backup(targetFileList);
        } else {
            logger.error("Person csv file not found");
        }
    }

    /**
     * 覆盖原来的comment文本
     *
     * @throws IOException
     */
    private void mergeContent(JavaSparkContext sc) throws IOException {
        List<File> targetFileList = getTargetFile(commentPath);
        if (null != targetFileList && targetFileList.size() > 0) {
            DataSelectStrategy dataSelectStrategy = DataSelectStrategyFactory.create(DatasetEnum.SENTIMENT_TEXT, sc);
            targetFileList.forEach(targetFile -> {
                try (
                        PrintWriter out = new PrintWriter(commentPath + "/" + targetFile.getName().replace(".csv", "-new.csv"));
                        BufferedReader reader = new BufferedReader(new FileReader(targetFile.getAbsolutePath()));
                ) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        SentimentText sentimentText = dataSelectStrategy.select();
                        String[] fields = line.split("\\|");
                        String newLine = "";
                        for (int i = 0; i < fields.length - 2; i++) {
                            newLine += fields[i] + "|";
                        }
                        out.println(String.format("%s\"%s\"|\"%d\"|\"%s\"", newLine, sentimentText.getText(), sentimentText.getText().length(), sentimentText.getTarget()));
                    }
                } catch (IOException e) {
                    logger.error("process comment file error", e);
                }
            });
            backup(targetFileList);
        } else {
            logger.error("Comment csv file not found");
        }

    }

    private void mergePost(JavaSparkContext sc) throws Exception {
        List<File> targetFileList = getTargetFile(postPath);
        if (null != targetFileList && targetFileList.size() > 0) {
            DataSelectStrategy dataSelectStrategy = DataSelectStrategyFactory.create(DatasetEnum.NEWS, sc);
            Map<String, String> postHasTopicMap = new HashMap<>();
            targetFileList.forEach(targetFile -> {
                try (
                        BufferedReader reader = new BufferedReader(new FileReader(targetFile.getAbsolutePath()));
                        BufferedWriter out = new BufferedWriter(new FileWriter(postPath + "/" + targetFile.getName().replace(".csv", "-new.csv")))
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
                        postHasTopicMap.put(originPostId, news.getTopic());
                    }
                } catch (Exception e) {
                    logger.error("process image file error", e);
                }
            });
            createTopic();
            createPostHasTopic(postHasTopicMap);
            backup(targetFileList);
        } else {
            logger.error("Post csv file not found");
        }
    }

//    private void mergePost(JavaSparkContext sc) throws Exception {
//        File targetFile = getTargetFile(postPath);
//        SparkSession spark = SparkSession.builder()
//                .appName("SparkSessionExample")
//                .master("local[*]") // 这里可以设置Spark的master节点，local[*]表示在本地运行，[*]表示使用所有可用的CPU核心
//                .getOrCreate();
//        Dataset<Row> df = spark.read()
//                .option("delimiter", "|")
//                .option("header", false) // 如果有表头，设置为true
//                .option("inferSchema", false) // 如果需要自动推断数据类型，设置为true
//                .csv(targetFile.getAbsolutePath());
//        DataSelectStrategy dataSelectStrategy = DataSelectStrategyFactory.create(DatasetEnum.NEWS,sc);
//
//        // 处理数据并创建新的DataFrame
//        Dataset<Row> processedDF = df.map((MapFunction<Row, Row>) row -> {
//            News news = dataSelectStrategy.select(); // 这里需要定义dataSelectStrategy
//
//            return RowFactory.create(row.getString(0),row.getString(1),row.getString(2),row.getString(3),row.getString(4),
//                    "en", news.getDescription(), news.getDescription().length(), news.getTopic());
//        }, RowEncoder.apply(new StructType()
//                .add("creationDate", DataTypes.StringType)
//                .add("id", DataTypes.StringType)
//                .add("imageFile", DataTypes.StringType)
//                .add("locationIP", DataTypes.StringType)
//                .add("browserUsed", DataTypes.StringType)
//                .add("language", DataTypes.StringType)
//                .add("content", DataTypes.StringType)
//                .add("length", DataTypes.LongType)
//                .add("topic", DataTypes.StringType)
//        ));
//
//        processedDF.write()
//                .option("delimiter", "|")
//                .option("header", false)
//                .csv(postPath+"/"+"new-part.csv");
//        JavaRDD<Row> rowJavaRDD = processedDF.sample(0.6, 42).javaRDD();
//        createTopic();
//        createPostHasTopic(rowJavaRDD);
//    }

    /**
     * @throws Exception
     */
    private void createTopic() throws Exception {
        File topicDir = new File(topicPath);
        File topicCsv = new File(topicPath + "/part-new.csv");
        if (!topicDir.exists()) {
            topicDir.mkdirs();
        }
        if (!topicCsv.exists()) {
            topicCsv.createNewFile();
        }
        try (
                BufferedWriter bw = new BufferedWriter(new FileWriter(topicCsv));
                BufferedReader br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(topicDictFileName), "UTF-8"));
        ) {
            String line;
            while ((line = br.readLine()) != null) {
                String labelNum = line.split("\\|")[0];
                String labelDesc = line.split("\\|")[1];
                bw.write(String.format("\"%s\"|\"%s\"\n", labelNum, labelDesc));
            }
            logger.info("createTopic finish!");
        }
    }

    private void createPostHasTopic(Map<String, String> postHasTopicMap) throws IOException {
        File postHasTopicDir = new File(postHasTopicPath);
        File postHasTopicCsv = new File(postHasTopicPath + "/part-new.csv");
        if (!postHasTopicDir.exists()) {
            postHasTopicDir.mkdirs();
        }
        if (!postHasTopicCsv.exists()) {
            postHasTopicCsv.createNewFile();
        }

        try (
                BufferedWriter bw = new BufferedWriter(new FileWriter(postHasTopicCsv));
        ) {
            List<Map.Entry<String, String>> entries = new ArrayList<>(postHasTopicMap.entrySet());
            Collections.shuffle(entries, new Random(42));

            int keepSize = (int) (entries.size() * 0.6);
            for (int i = 0; i < keepSize; i++) {
                bw.write(String.format(String.format("%s|\"%s\"\n", entries.get(i).getKey(), entries.get(i).getValue())));
            }
            logger.info("create postHasTopic finish!");
        } catch (IOException e) {
            logger.error("write postHasTopic file error", e);
        }
    }

//    private void createPostHasTopic(JavaRDD<Row> postHasTopicMap){
//        try(
//                BufferedWriter bw = new BufferedWriter(new FileWriter(postHasTopicPath));
//        ){
//            postHasTopicMap.foreach(e->{
//                bw.write(String.format(String.format("\"%s\"|\"%s\"\n",e.getString(1), e.get(8))));
//            });
//            logger.info("create postHasTopic finish!");
//        } catch (IOException e) {
//            logger.error("write postHasTopic file error",e);
//        }
//    }


    /**
     * 获取ldbc csv文件
     *
     * @param parentDir
     * @return
     */
    private List<File> getTargetFile(String parentDir) {
        File[] files = new File(parentDir).listFiles();
        List<File> targetFileList = new ArrayList<>();
        for (File file : files) {
            if (file.getName().startsWith("part") && file.getName().endsWith("csv") && !file.getName().contains("new")) {
                targetFileList.add(file);
            }
        }
        return targetFileList;
    }

    private void backup(List<File> fileList) {
        fileList.forEach(file -> {
            file.renameTo(new File(file.getParent() + "/back-" + file.getName()));
        });
    }

    public static void main(String[] args) {
        Gen main = new Gen();
        main.merge();
    }
}
