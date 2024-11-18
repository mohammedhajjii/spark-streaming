package md.hajji.incidents;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;
import java.util.concurrent.TimeoutException;
import static org.apache.spark.sql.functions.*;

public class IncidentAnalyser {

    private static final String APP_NAME = "IncidentAnalyser";
    private static final String SPARK_MASTER = "spark://spark-master:7077";
    private static final String FILES_LOCATION = "hdfs://namenode:8020/incidents";
    private static final String LOG_LEVEL = "WARN";

    /**
     * create spark session
     * @return : SparkSession instance
     */
    static SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName(APP_NAME)
                .master(SPARK_MASTER)
                .getOrCreate();
    }

    /**
     * define incident schema
     * @return : StructType instance
     */
    static StructType getIncidentSchema() {
        return new StructType(
                new StructField[]{
                        new StructField("Id", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField("title", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("description", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("service", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("date", DataTypes.DateType, false, Metadata.empty()),
                }
        );
    }

    public static void main(String[] args) {


        // initialize spark session
        try(SparkSession session = getSparkSession()){

            // set up logs level enable only  Warning logs:
            session.sparkContext().setLogLevel(LOG_LEVEL);

            // connect to incidents directory where incidents csv files are stored:
            Dataset<Row> incidents = session.readStream()
                    // define incident schema:
                    .schema(getIncidentSchema())
                    // tell spark that first row is the header:
                    .option("header", "true")
                    // load csv files from incidents directory location in HDFS:
                    .csv(FILES_LOCATION);


            // define the first query that count incidents by service:
            // group by service col:
            StreamingQuery countIncidentByService = incidents.groupBy(col("service"))
                     // use count aggregation func and rename count column:
                    .agg(count("*").alias("Number_incidents"))
                    // write the results in the output table:
                    .writeStream()
                    //set output mode to Complete so the entire query results will be
                    //persisted to the output:
                    .outputMode(OutputMode.Complete())
                    // we need to persist results in console:
                    .format("console")
                    // start the query:
                    .start();

            // define the second query that most 2 year when we have max incidents:
            StreamingQuery mostIncidentByYear = incidents
                    // group by year col parsed from date col:
                    // alias for rename column name in the output table:
                    .groupBy(year(col("date")).alias("Year_incidents"))
                    // apply count aggregation func:
                    .agg(count("*")
                            .alias("Number_incidents"))
                    // order results by number of incidents (desc order):
                    .orderBy(col("Number_incidents").desc())
                    // we need just 2 first years:
                    .limit(2)
                    // write the results in the output table with complete mode:
                    .writeStream()
                    .outputMode(OutputMode.Complete())
                    // the results will be persisted in the console:
                    .format("console")
                    .start();


            // await two queries to finish:
            countIncidentByService.awaitTermination();
            mostIncidentByYear.awaitTermination();


        } catch (TimeoutException | StreamingQueryException exception) {
            throw new RuntimeException(exception);
        }
    }
}
