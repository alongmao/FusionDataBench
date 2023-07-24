package entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/7/23 21:35
 * @Version 1.0
 */
@Data
public class News implements Serializable {
    private String topic;
    private String description;
}
