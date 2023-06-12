package entity;

import java.io.Serializable;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/9 14:33
 * @Version 1.0
 */
public class SentimentText implements Serializable {

    private String target;

    private String ids;

    private String date;

    private String user;

    private String text;

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getIds() {
        return ids;
    }

    public void setIds(String ids) {
        this.ids = ids;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return "SentimentText{" +
                "target='" + target + '\'' +
                ", ids='" + ids + '\'' +
                ", date='" + date + '\'' +
                ", user='" + user + '\'' +
                ", text='" + text + '\'' +
                '}';
    }
}
