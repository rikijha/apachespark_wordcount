package sparkdemo.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App 
{
    public static void main( String[] args )
    {
       
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf sparkConf=new SparkConf().setAppName("Word Count").setMaster("local[*]");
        JavaSparkContext context=new JavaSparkContext(sparkConf);
       
        List<Integer> inputData=new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);
        
        
        JavaRDD<Integer> myRDD=context.parallelize(inputData);
        Integer result=myRDD.reduce((v1,v2) -> v1+v2);
        
         JavaRDD<Double>  newRDD=myRDD.map(value -> Math.sqrt(value));
         
         newRDD.collect().forEach(System.out::println);
        
        System.out.println(result);
        context.close();
    }
}
