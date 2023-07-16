package task.fusionDB;

import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import util.HttpRequest;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/25 22:23
 * @Version 1.0
 */
@Slf4j
public class AIService {

    private final static String image_similarity_url = "http://127.0.0.1:5000/image/similarity";

    private final static String sentiment_predict_url = "http://127.0.0.1:5000/sentiment/predict";


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
}
