import entity.Face;
import entity.SentimentText;
import enums.DatasetEnum;
import factory.DataSelectStrategyFactory;
import org.apache.log4j.Logger;
import strategy.DataSelectStrategy;

import java.io.*;

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


    public void merge() {
        try {
            mergeFace();
            mergeContent();
        } catch (IOException e) {
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
            PrintWriter out = new PrintWriter(personPath + "/part-new1.csv");
            while ((line = reader.readLine()) != null) {
                String faceImagePath = ((Face) dataSelectStrategy.select()).getFacePath();
                out.println(line + "|" + faceImagePath);
            }
            out.close();
            reader.close();
        } else {
            logger.info("Person csv file not found");
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
            PrintWriter out = new PrintWriter(commentPath + "/part-new1.csv");
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
            logger.info("Comment csv file not found");
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
            if (file.getName().endsWith(".csv")) {
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
