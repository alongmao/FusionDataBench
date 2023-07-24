package strategy;

import com.alibaba.fastjson2.JSONObject;
import entity.SentimentText;
import org.apache.log4j.Logger;

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

    private  final String sentiTextFilePath = "/Users/along/Documents/dataset/TweetDataset/eda_sentiment1440.csv";

    List<SentimentText> sentimentTextList;

    private Integer index;

    public SentiTextSelectStrategy() {
        index = 0;
        this.sentimentTextList = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(sentiTextFilePath));
            String line;
            while((line=reader.readLine())!=null){
                SentimentText sentimentText = new SentimentText();
                int splitIndex = line.indexOf("\t");
                String label = line.substring(0,splitIndex);
                String content = line.substring(splitIndex+1);
                sentimentText.setTarget(label);
                sentimentText.setText(content);
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
