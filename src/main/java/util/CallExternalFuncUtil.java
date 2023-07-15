package util;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/18 21:21
 * @Version 1.0
 */
@Slf4j
public class CallExternalFuncUtil {

    private static final String PYTHON = "/Users/along/opt/miniforge3/envs/dl/bin/python3";

    private static final String IMAGE_AI_SCRIPT = "/Users/along/Downloads/spark-demo/src/main/java/script/image_identify.py";

    private static final String NLP_AI_SCRIPT = "/Users/along/Downloads/spark-demo/src/main/java/script/sentiment_identify.py";

    public static String callImageRecognition(String originPath, String targetPath) {
        // 构建进程构建器
        ProcessBuilder pb = new ProcessBuilder(PYTHON, IMAGE_AI_SCRIPT, originPath, targetPath);
        return callExternalApp(pb);
    }

    public static String callSentimentRecognition(String text){
        if(text==null)  {
            return "1,1";
        }
        ProcessBuilder pb = new ProcessBuilder(PYTHON,NLP_AI_SCRIPT,text);
        return callExternalApp(pb);
    }

    private static String callExternalApp(ProcessBuilder pb){
        pb.redirectErrorStream(true);

        // 启动进程
        Process process = null;
        long t1 = System.currentTimeMillis();
        long t2 = System.currentTimeMillis();
        try {
            process = pb.start();
        } catch (IOException e) {
            e.printStackTrace();
            return "0";
        }

        // 获取进程输出
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String rs = "";
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                rs += line;
            }
            // 等待进程结束
            int exitCode = process.waitFor();
            process.destroy();
        } catch (Exception e) {
            e.printStackTrace();
            return "0";
        }
        t2 = System.currentTimeMillis();
        log.info("call external app cost {}ms",(t2-t1));
        return rs.trim();
    }
}
