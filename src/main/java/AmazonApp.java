import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.size;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/5/15 11:50
 * @Version 1.0
 */
public class AmazonApp {
    static final String metadataPath = "/Users/along/Desktop/AmazonDataset/metadata/data";
    static final String reviewPath = "/Users/along/Desktop/AmazonDataset/review/data";
    static final String outputPath = "/Users/along/Desktop/AmazonDataset/output";

    public static void main(String[] args) throws AnalysisException {
        long t1 = System.currentTimeMillis();
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Amazon App");
        SparkSession spark = SparkSession.builder().appName("Amazon Dataset Process App").config(sparkConf).getOrCreate();

        StructType metaDataSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("asin", DataTypes.StringType, false),
                DataTypes.createStructField("price", DataTypes.StringType, false),
                DataTypes.createStructField("brand", DataTypes.StringType, false),
                DataTypes.createStructField("title", DataTypes.StringType, false),
                DataTypes.createStructField("imageURLHighRes", DataTypes.StringType, false)
        });

        StructType reviewSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("overall", DataTypes.FloatType, false),
                DataTypes.createStructField("reviewerID", DataTypes.StringType, false),
                DataTypes.createStructField("asin", DataTypes.StringType, false),
                DataTypes.createStructField("reviewText", DataTypes.StringType, false),
                DataTypes.createStructField("summary", DataTypes.StringType, false),
                DataTypes.createStructField("unixReviewTime", DataTypes.StringType, false),
        });


        Dataset<Row> metaPhoneDf = spark.read().schema(metaDataSchema).json(String.format("%s/%s", metadataPath, "meta_Cell_Phones_and_Accessories.json"));
        Dataset<Row> reviewPhoneDf = spark.read().schema(reviewSchema).json(String.format("%s/%s", reviewPath, "Cell_Phones_and_Accessories_5.json"));

        Dataset<Row> metaClothDf = spark.read().schema(metaDataSchema).json(String.format("%s/%s", metadataPath, "meta_Clothing_Shoes_and_Jewelry.json"));
        Dataset<Row> reviewClothDf = spark.read().schema(reviewSchema).json(String.format("%s/%s", reviewPath, "Clothing_Shoes_and_Jewelry_5.json"));

        Dataset<Row> metaFoodDf = spark.read().schema(metaDataSchema).json(String.format("%s/%s", metadataPath, "meta_Grocery_and_Gourmet_Food.json"));
        Dataset<Row> reviewFoodDf = spark.read().schema(reviewSchema).json(String.format("%s/%s", reviewPath, "Grocery_and_Gourmet_Food_5.json"));

        Dataset<Row> metaHomeDf = spark.read().schema(metaDataSchema).json(String.format("%s/%s", metadataPath, "meta_Home_and_Kitchen.json"));
        Dataset<Row> reviewHomeDf = spark.read().schema(reviewSchema).json(String.format("%s/%s", reviewPath, "Home_and_Kitchen_5.json"));

        Dataset<Row> metaMusicalDf = spark.read().schema(metaDataSchema).json(String.format("%s/%s", metadataPath, "meta_Musical_Instruments.json"));
        Dataset<Row> reviewMusicalDf = spark.read().schema(reviewSchema).json(String.format("%s/%s", reviewPath, "Musical_Instruments_5.json"));

        Dataset<Row> metaToolsDf = spark.read().schema(metaDataSchema).json(String.format("%s/%s", metadataPath, "meta_Tools_and_Home_Improvement.json"));
        Dataset<Row> reviewToolsDf = spark.read().schema(reviewSchema).json(String.format("%s/%s", reviewPath, "Tools_and_Home_Improvement_5.json"));

        /**
         * metadata
         * 1. image 个数大于0
         * 2. price 不等于null
         * 3. brand 不等于null
         */
        metaPhoneDf = metaPhoneDf.filter(col("imageURLHighRes").notEqual("[]").and(col("price").notEqual("")).and(col("brand").notEqual("")));
        metaClothDf = metaClothDf.filter(col("imageURLHighRes").notEqual("[]").and(col("price").notEqual("")).and(col("brand").notEqual("")));
        metaFoodDf = metaFoodDf.filter(col("imageURLHighRes").notEqual("[]").and(col("price").notEqual("")).and(col("brand").notEqual("")));
        metaHomeDf = metaHomeDf.filter(col("imageURLHighRes").notEqual("[]").and(col("price").notEqual("")).and(col("brand").notEqual("")));
        metaMusicalDf = metaMusicalDf.filter(col("imageURLHighRes").notEqual("[]").and(col("price").notEqual("")).and(col("brand").notEqual("")));
        metaToolsDf = metaToolsDf.filter(col("imageURLHighRes").notEqual("[]").and(col("price").notEqual("")).and(col("brand").notEqual("")));

//        metaPhoneDf.show();
//        reviewPhoneDf.show();
//        metaClothDf.show();
//        reviewClothDf.show();
//        metaFoodDf.show();
//        reviewFoodDf.show();
//        metaHomeDf.show();
//        reviewHomeDf.show();
//        metaMusicalDf.show();
//        reviewMusicalDf.show();
//        metaToolsDf.show();
//        reviewToolsDf.show();

        /**1. 创建表
         * 2. meta表与review表执行外连接
         */
        metaPhoneDf.createGlobalTempView("metaPhone");
        reviewPhoneDf.createGlobalTempView("reviewPhone");
        metaClothDf.createGlobalTempView("metaCloth");
        reviewClothDf.createGlobalTempView("reviewCloth");
        metaFoodDf.createGlobalTempView("metaFood");
        reviewFoodDf.createGlobalTempView("reviewFood");
        metaHomeDf.createGlobalTempView("metaHome");
        reviewHomeDf.createGlobalTempView("reviewHome");
        metaMusicalDf.createGlobalTempView("metaMusical");
        reviewMusicalDf.createGlobalTempView("reviewMusical");
        metaToolsDf.createGlobalTempView("metaTools");
        reviewToolsDf.createGlobalTempView("reviewTools");

        Dataset<Row> phoneDf = spark.sql("select m.asin as asin,price,brand,title,imageURLHighRes,overall,reviewerID,reviewText,summary,unixReviewTime from global_temp.metaPhone m, global_temp.reviewPhone r where m.asin = r.asin");
        Dataset<Row> clothDf = spark.sql("select m.asin as asin,price,brand,title,imageURLHighRes,overall,reviewerID,reviewText,summary,unixReviewTime from global_temp.metaCloth m, global_temp.reviewCloth r where m.asin = r.asin");
        Dataset<Row> foodDf = spark.sql("select m.asin as asin,price,brand,title,imageURLHighRes,overall,reviewerID,reviewText,summary,unixReviewTime from global_temp.metaFood m, global_temp.reviewFood r where m.asin = r.asin");
        Dataset<Row> homeDf = spark.sql("select m.asin as asin,price,brand,title,imageURLHighRes,overall,reviewerID,reviewText,summary,unixReviewTime from global_temp.metaHome m, global_temp.reviewHome r where m.asin = r.asin");
        Dataset<Row> musicalDf = spark.sql("select m.asin as asin,price,brand,title,imageURLHighRes,overall,reviewerID,reviewText,summary,unixReviewTime from global_temp.metaMusical m, global_temp.reviewMusical r where m.asin = r.asin");
        Dataset<Row> toolsDf = spark.sql("select m.asin as asin,price,brand,title,imageURLHighRes,overall,reviewerID,reviewText,summary,unixReviewTime from global_temp.metaTools m, global_temp.reviewTools r where m.asin = r.asin");


        /**
         * 重新分离meta表和review表，经过上面连接处理后 保证了每一条评论对有对应的商品记录
         */

        Dataset<Row> phoneProduct = phoneDf.select(col("asin"), col("price"), col("brand"), col("title"), col("imageURLHighRes"));
        Dataset<Row> phoneFeedback = phoneDf.select(col("asin"), col("reviewerID"), col("reviewText"), col("summary"), col("unixReviewTime"), col("overall"));

        Dataset<Row> clothProduct = clothDf.select(col("asin"), col("price"), col("brand"), col("title"), col("imageURLHighRes"));
        Dataset<Row> clothFeedback = clothDf.select(col("asin"), col("reviewerID"), col("reviewText"), col("summary"), col("unixReviewTime"), col("overall"));

        Dataset<Row> homeProduct = homeDf.select(col("asin"), col("price"), col("brand"), col("title"), col("imageURLHighRes"));
        Dataset<Row> homeFeedback = homeDf.select(col("asin"), col("reviewerID"), col("reviewText"), col("summary"), col("unixReviewTime"), col("overall"));

        Dataset<Row> foodProduct = foodDf.select(col("asin"), col("price"), col("brand"), col("title"), col("imageURLHighRes"));
        Dataset<Row> foodFeedback = foodDf.select(col("asin"), col("reviewerID"), col("reviewText"), col("summary"), col("unixReviewTime"), col("overall"));

        Dataset<Row> musicalProduct = musicalDf.select(col("asin"), col("price"), col("brand"), col("title"), col("imageURLHighRes"));
        Dataset<Row> musicalFeedback = musicalDf.select(col("asin"), col("reviewerID"), col("reviewText"), col("summary"), col("unixReviewTime"), col("overall"));

        Dataset<Row> toolsProduct = toolsDf.select(col("asin"), col("price"), col("brand"), col("title"), col("imageURLHighRes"));
        Dataset<Row> toolsFeedback = toolsDf.select(col("asin"), col("reviewerID"), col("reviewText"), col("summary"), col("unixReviewTime"), col("overall"));


        /**
         * 保存数据为csv格式
         */
        phoneProduct.write().format("csv").save(outputPath+"/phone.csv");
        phoneFeedback.write().format("csv").save(outputPath+"/phone_feedback.csv");

        clothProduct.write().format("csv").save(outputPath+"/cloth.csv");
        clothFeedback.write().format("csv").save(outputPath+"/cloth_feedback.csv");

        homeProduct.write().format("csv").save(outputPath+"/home.csv");
        homeFeedback.write().format("csv").save(outputPath+"/home_feedback.csv");

        foodProduct.write().format("csv").save(outputPath+"/food.csv");
        foodFeedback.write().format("csv").save(outputPath+"/food_feedback.csv");

        musicalProduct.write().format("csv").save(outputPath+"/musical.csv");
        musicalFeedback.write().format("csv").save(outputPath+"/musical_feedback.csv");

        toolsProduct.write().format("csv").save(outputPath+"/tools.csv");
        toolsFeedback.write().format("csv").save(outputPath+"/tools_feedback.csv");

        /**
         * 记录生成数据的耗时和数量
         */
        long t2 = System.currentTimeMillis();
        long cnt = 0;
        cnt += phoneFeedback.count() + clothFeedback.count() + foodFeedback.count() +
                homeFeedback.count() + musicalFeedback.count() + toolsFeedback.count();
        System.out.println(String.format("cost %d ms ,total record: %d",t2-t1,cnt));
    }
}
