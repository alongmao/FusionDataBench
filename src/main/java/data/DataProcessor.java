package data;

import strategy.DataSelectStrategy;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/9 14:13
 * @Version 1.0
 */
public interface DataProcessor {

    void process(String dataDir);

    <T> T select(DataSelectStrategy selectStrategy);
}
