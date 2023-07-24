package entity;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/9 14:34
 * @Version 1.0
 */

@Data
public class Face implements Serializable {
    private String facePath;
}
