package sparkdemo.spark;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App 
{
    public static void main( String[] args )
    {
       
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf=new SparkConf().setAppName("Word Count").setMaster("local");
        JavaSparkContext context=new JavaSparkContext(sparkConf);
        JavaRDD<String> lines=context.textFile("C:\\Users\\Dell\\Documents\\workspace-sts-3.9.5.RELEASE\\spark\\data\\hello.txt");
        JavaRDD<String> words=lines.flatMap(s->Arrays.asList(s.split(" ")));
        Map<String, Long> counts=words.countByValue();
        for(Map.Entry<String, Long> entry:counts.entrySet()) {
        	System.out.println(entry.getKey()+" : "+entry.getValue());
        }
    }
}
