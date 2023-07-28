package task.fusionDB;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.json4s.jackson.Json;
import util.HttpRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/25 22:23
 * @Version 1.0
 */
@Slf4j
public class AIService {

    private final static String image_similarity_url = "http://10.0.82.211:5000/image/similarity";

    private final static String sentiment_predict_url = "http://10.0.82.211:5000/sentiment/predict";

    private final static String topic_extract_url = "http://10.0.82.211:5000/topic/extract";


    public static double similarity(String originImagePath, String targetImagePath) {
        try {
            JSONObject result = JSONObject.parseObject(HttpRequest.sendGet(image_similarity_url, String.format("origin=%s&target=%s", originImagePath, targetImagePath)));
            return Double.parseDouble(result.get("similarity").toString());
        } catch (Exception e) {
            log.error("AIService invoke similarity error ", e);
        }
        return 0.0;
    }

    public static int classifySenti(String text) {
        try {
            JSONObject param = new JSONObject();
            param.put("text", text);
            JSONObject result = JSONObject.parseObject(HttpRequest.sendPost(sentiment_predict_url, JSONObject.toJSONString(param)));
            return Integer.parseInt(result.get("sentiment_type").toString());
        } catch (Exception e) {
            log.error("AIService invoke classifySenti error ", e);
        }
        return 1;
    }

    public static List<Integer> extractTopic(List<String> textList){
        try{
            JSONObject param = new JSONObject();
            param.put("texts",textList);
            JSONObject response = JSONObject.parseObject(HttpRequest.sendPost(topic_extract_url, JSONObject.toJSONString(param)));

            List<Integer> topicList = new ArrayList<>();
            JSONArray data = response.getJSONArray("data");
            for(int i=0;i<data.size();i++){
                JSONObject object = data.getJSONObject(i);
                topicList.add(Integer.parseInt(object.get("topic_id").toString()));
            }
            return topicList;
        }catch (Exception e){
            log.error("AIService invoke extractTopic error ",e);
        }
        return null;
    }
}
