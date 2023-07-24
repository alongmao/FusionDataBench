package entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/9 14:33
 * @Version 1.0
 */

@Data
public class SentimentText implements Serializable {

    private String target;

    private String ids;

    private String date;

    private String user;

    private String text;

}
