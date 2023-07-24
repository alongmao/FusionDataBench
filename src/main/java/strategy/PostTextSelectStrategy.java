package strategy;

import com.alibaba.fastjson2.JSONObject;
import entity.News;
import entity.SentimentText;
import org.apache.log4j.Logger;
import org.apache.spark.sql.sources.In;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/7/23 21:34
 * @Version 1.0
 */
public class PostTextSelectStrategy implements DataSelectStrategy {
    Logger logger = Logger.getLogger(PostTextSelectStrategy.class);

    private final String dir = "/Users/along/Documents/dataset/AGNews";

    private final String[] files = {"eda_train.txt", "eda_test.txt","eda_dev.txt"};

    private Integer index;
    List<News> newsList;

    public PostTextSelectStrategy() {
        index = 0;
        this.newsList = new ArrayList<>();
        try {
            for (String file : files) {
                BufferedReader reader = new BufferedReader(new FileReader(String.format("%s/%s", dir, file)));
                String line;
                while ((line = reader.readLine()) != null) {
                    int splitIndex = line.indexOf("\t");
                    String label = line.substring(0,splitIndex);
                    String content = line.substring(splitIndex+1);
                    News news = new News();
                    news.setClassIndex(label);
                    news.setDescription(content);
                    this.newsList.add(news);
                }
                Collections.shuffle(this.newsList, new Random(42));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public News select() {
        if (index < this.newsList.size()) {
            return this.newsList.get(index++);
        }
        logger.info("select Post text fail");
        return null;
    }
}

