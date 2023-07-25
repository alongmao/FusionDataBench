package strategy;

import com.alibaba.fastjson2.JSONObject;
import entity.SentimentText;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.*;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/9 15:28
 * @Version 1.0
 */
public class SentiTextSelectStrategy implements DataSelectStrategy, Serializable {

    transient Logger logger = Logger.getLogger(SentiTextSelectStrategy.class);

    private final String datasetDir = System.getenv("MULTIMODAL_DATASET_DIR");

    private final String sentiTextFilePath = datasetDir + "/TweetDataset/eda_sentiment1440.csv";

    List<SentimentText> sentimentTextList;

    private Integer index;

    private transient Iterator<SentimentText> sentimentTextIt;

    public SentiTextSelectStrategy(JavaSparkContext sc) {
        JavaRDD<SentimentText> sentimentTextRDD = sc.textFile(sentiTextFilePath)
                .map(line -> {
                    SentimentText sentimentText = new SentimentText();
                    int splitIndex = line.indexOf("\t");
                    String label = line.substring(0, splitIndex);
                    String content = line.substring(splitIndex + 1);
                    sentimentText.setTarget(label);
                    sentimentText.setText(content);
                    return sentimentText;
                });
        this.sentimentTextIt = sentimentTextRDD.toLocalIterator(); // 这里示意随机采样，采样比例为50%
    }

    @Override
    public SentimentText select() {
        if (sentimentTextIt.hasNext()) {
            return sentimentTextIt.next();
        }
        logger.info("select sentiment text fail");
        return null;
    }

}
