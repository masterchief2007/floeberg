package com.masterchief;


import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Arrays;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Floeberg fb = new Floeberg();

        Table companyTable;
        String fileNamePrefix;

        fileNamePrefix= getFileNamePrefix();
        setupTestDirectories();

        try {
            companyTable = fb.loadTableMetadata();
        }
        catch(NoSuchTableException nste) {
            System.out.println("Creating Metadata ....");
            companyTable = fb.setupCompanyTable();
        }

        SparkSession  spark = fb.getSparkSession();

        Dataset<Row> datasetOfRows = fb.getTestData(10);

        // Take a look at the test data...
        datasetOfRows.show();
        datasetOfRows.printSchema();

        // Parquet...
        saveAsParquet(datasetOfRows, fileNamePrefix);
        Dataset<Row> ds = readAsParquet(spark, fileNamePrefix);

        // Iceberg...
        saveInIceberg(ds, companyTable);
        readFromIceberg();
    }

    private static String getFileNamePrefix() {

        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .appendLiteral("test-")
                .appendValue(ChronoField.YEAR)
                .appendLiteral("-")
                .appendValue(ChronoField.MONTH_OF_YEAR)
                .appendLiteral("-")
                .appendValue(ChronoField.DAY_OF_MONTH)
                .appendLiteral("_")
                .appendValue(ChronoField.MINUTE_OF_DAY)
                .appendLiteral(".pq")
                .toFormatter();

        return  LocalDateTime.now().format(formatter);

    }

    private static void saveInIceberg(Dataset<Row> datasetOfRows, Table company) {
        System.out.println("Saving same Data in Iceberg ........");

        datasetOfRows.write()
                .format("iceberg")
                .mode("append")
                .save(company.location());

    }

    private static void readFromIceberg() {
        System.out.println("To be done....");

    }
    private static void saveAsParquet(Dataset<Row> datasetOfRows, String filePrefix) {
        System.out.println("Saving data as Parquet file......");
        datasetOfRows.write().format("parquet").save("target/parquet-out/" + filePrefix);
    }

    private static Dataset<Row> readAsParquet(SparkSession spark, String filePrefix) {
        Dataset<Row>  ds = spark.read().parquet("target/parquet-out/" + filePrefix);
        ds.show();
        return ds;
    }

    private static void setupTestDirectories() {

        String[]  dirs = {"target/logs", "target/parquet-out"};

        Arrays.asList(dirs).stream().forEach(d -> {

            File target = new File(d );
            if(!target.exists()){
                if( !target.mkdirs()) {
                    throw new UnknownError("Error Creating .. " + d);
                }
            }

        });

    }
}
