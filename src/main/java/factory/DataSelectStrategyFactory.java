package factory;

import java.util.HashMap;
import java.util.Map;
import enums.DatasetEnum;
import org.apache.spark.api.java.JavaSparkContext;
import strategy.*;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/9 14:59
 * @Version 1.0
 */
public class DataSelectStrategyFactory {
    private static Map<String, DataSelectStrategy> factory = new HashMap<>();

    public static DataSelectStrategy create(DatasetEnum datasetEnum, JavaSparkContext sc){
        switch (datasetEnum){
            case PRODUCT:{
                    if(null==factory.get(DatasetEnum.PRODUCT.getName())){
                        synchronized (ProductSelectStrategy.class){
                            factory.put(DatasetEnum.PRODUCT.getName(), new ProductSelectStrategy());
                        }
                    }
                    break;
            }
            case REVIEW:{
                if(null == factory.get(DatasetEnum.REVIEW.getName())){
                    synchronized (ReviewSelectStrategy.class){
                        factory.put(DatasetEnum.REVIEW.getName(), new ReviewSelectStrategy());
                    }
                }
                break;
            }
            case SENTIMENT_TEXT:{
                if(null == factory.get(DatasetEnum.SENTIMENT_TEXT.getName())){
                    synchronized (SentiTextSelectStrategy.class){
                        factory.put(DatasetEnum.SENTIMENT_TEXT.getName(), new SentiTextSelectStrategy(sc));
                    }
                }
                break;
            }
            case FACE:{
                if(null==factory.get(DatasetEnum.FACE.getName())){
                    synchronized (FaceSelectStrategy.class){
                        factory.put(DatasetEnum.FACE.getName(), new FaceSelectStrategy(sc));
                    }
                }
                break;
            }
            case NEWS:{
                if(null==factory.get(DatasetEnum.NEWS.getName())){
                    synchronized (PostTextSelectStrategy.class){
                        factory.put(DatasetEnum.NEWS.getName(), new PostTextSelectStrategy(sc));
                    }
                }
                break;
            }
        }
        return factory.get(datasetEnum.getName());
    }
}
