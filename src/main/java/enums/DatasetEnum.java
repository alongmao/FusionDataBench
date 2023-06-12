package enums;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/9 15:01
 * @Version 1.0
 */
public enum DatasetEnum {

    PRODUCT("商品元数据"),

    REVIEW("商品评论数据"),

    SENTIMENT_TEXT("情感文本数据"),

    FACE("人脸数据");

    private String name;

    DatasetEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
