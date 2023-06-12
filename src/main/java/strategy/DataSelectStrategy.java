package strategy;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/9 14:54
 * @Version 1.0
 */
public interface DataSelectStrategy {
    <T> T select();
}
