package util;

import core.MixQuery;
import org.apache.spark.sql.DataFrame;

/**
 * Created by lzz on 17/5/1.
 */
public class Service {
    public static void main(String[] args){
        System.out.println("hello world!");
        MixQuery mixQuery = new MixQuery();
        DataFrame rdd = mixQuery.startJob();
        rdd.show();
        System.out.println(rdd);
    }

    public void process(){
        MixQuery mixQuery = new MixQuery();
        DataFrame rdd = mixQuery.startJob();
        rdd.show();
        System.out.println(rdd);
    }
}
