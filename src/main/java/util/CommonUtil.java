package util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/20 23:09
 * @Version 1.0
 */
public class CommonUtil {

    /**
     * 无limit获取
     * @param it
     * @param predicate
     * @param <T>
     * @return
     */
    public static <T> List<T> convertIterator2List(Iterator<T> it, Predicate<T> predicate) {
        return convertIterator2List(it,predicate,-1);
    }

    /**
     * limit控制早停
     * @param it
     * @param predicate
     * @param limit
     * @param <T>
     * @return
     */
    public static <T> List<T> convertIterator2List(Iterator<T> it, Predicate<T> predicate,int limit) {
        ArrayList<T> list = new ArrayList<T>();
        while (it.hasNext()) {
            T t = it.next();
            if(predicate.test(t)){
                list.add(t);
                limit--;
            }
            if(limit==0){
                return list;
            }
        }
        return list;
    }

}
