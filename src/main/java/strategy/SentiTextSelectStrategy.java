package strategy;

import com.alibaba.fastjson2.JSONObject;
import entity.SentimentText;
import org.apache.arrow.flatbuf.Int;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.io.*;
import java.util.*;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/9 15:28
 * @Version 1.0
 */
public class SentiTextSelectStrategy implements DataSelectStrategy {

    Logger logger = Logger.getLogger(SentiTextSelectStrategy.class);

    private  final String sentiTextFilePath = "/Users/along/Documents/dataset/TweetDataset/sentiment160.json";

    List<SentimentText> sentimentTextList;

    private Integer index;

    public SentiTextSelectStrategy() {
        index = 0;
        this.sentimentTextList = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(sentiTextFilePath));
            String line;
            while((line=reader.readLine())!=null){
                SentimentText sentimentText = JSONObject.parseObject(line, SentimentText.class);
                this.sentimentTextList.add(sentimentText);
            }
            Collections.shuffle(this.sentimentTextList,new Random(42));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public SentimentText select() {
        if(index<this.sentimentTextList.size()){
            return this.sentimentTextList.get(index++);
        }
        logger.info("select sentiment text fail");
        return null;
    }

}
