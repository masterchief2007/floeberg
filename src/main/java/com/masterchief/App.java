package com.masterchief;


import com.masterchief.data.Company;
import com.mw.commons.AWSManager;
import com.mw.commons.DataLakeConfiguration;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

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

        System.out.println(AWSManager.getInstance().getAwsKey());

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

        Dataset<Row> datasetOfRows = fb.getTestDataAsRows(10);

        // Take a look at the test data...
        datasetOfRows.show();
        datasetOfRows.printSchema();

        // Parquet...
        saveAsParquet(datasetOfRows, fileNamePrefix);
        Dataset<Row> ds = readAsParquet(spark, fileNamePrefix);

        // Iceberg...
//        saveInIceberg(ds, companyTable);// Didnt Work as after read, it still reads as ID optional
        saveInIceberg(datasetOfRows, companyTable);

        Dataset<Row> readData= readFromIceberg(spark, companyTable);

        readData = prepareForUpdate( spark, companyTable, readData);

        saveInIceberg(readData, companyTable);

        // SELECT Query on the Updated Data
        // readUpdatedFromIceberg(spark, companyTable); // Didnt work
        checkUpdated(spark, companyTable);

        spark.stop();
    }

    private static void readUpdatedFromIceberg(SparkSession spark, Table companyTable) {

        System.out.println("---------------------------------------");
        System.out.println("Querying Iceberg for updated data......");
        System.out.println("---------------------------------------");

        Dataset<Row> updated= spark.sql("SELECT * FROM iceberg.company where date_updated != 0");
        System.out.println("Data Read from Iceberg and the updated count is " + updated.count());
        updated.show();
    }

    private static Dataset<Row> prepareForUpdate(SparkSession spark, Table companyTable, Dataset<Row> companies) {
        System.out.println("---------------------------------------");
        System.out.println("Preparing for update...................");
        System.out.println("---------------------------------------");

        Dataset<Row> toUpdate = companies.where("date_created=1553842631124L");
        Dataset<Row> others= companies.except(toUpdate);
        Dataset<Row> updated= toUpdate.withColumn("date_updated",
                                            functions.lit( System.currentTimeMillis()));
        return  others.union (updated);
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

    /*
    Saves Dataset in Parquet by Default (in Iceberg)
     */
    private static void saveInIceberg(Dataset<Row> datasetOfRows, Table company) {
        System.out.println("Saving same Data in Iceberg ........");

        datasetOfRows.write()
                .format("iceberg")
                .mode("append")
                .save(company.location());

    }

    private static void checkUpdated(SparkSession spark, Table company) {

        Dataset<Row> ds = readFromIceberg(spark, company);
        System.out.println("CheckUpdated - Finished Reading.");
        Dataset<Row> updated= ds.where("date_created=1553842631124L");
        System.out.println("----------------------------------------------");
        System.out.println("Number of Records Updated = " + updated.count());
        System.out.println("----------------------------------------------");

        updated.show();
    }


    private static Dataset<Row> readFromIceberg(SparkSession spark, Table company) {
        System.out.println("Read To be done....");

        Dataset<Row> ds = spark.read().format("iceberg").load(company.location());

        ds.show();

        System.out.println("Total Rows read from Iceberg = " + ds.count());
        return ds;
    }

    private static Dataset<Company> readFromIcebergAsCompany(SparkSession spark, Table company) {
        System.out.println("Read To be done....");

        Dataset<Company> ds = spark.read().format("iceberg").load(company.location()).as(Company.getEncoder());

        ds.show();

        System.out.println("Total Companies read from Iceberg = " + ds.count());
        return ds;
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

        String[]  dirs = {"target/logs", "target/parquet-out", "target/data"};

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
