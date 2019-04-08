package com.masterchief;

import com.masterchief.data.Company;
import com.mw.commons.AWSManager;
import com.mw.commons.DataLakeConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class Floeberg {

    private Configuration conf;
    private String locationOfTable = "target/data";

    private boolean isS3Based;

    private HadoopTables hTables ;
    Schema  companySchema;


    public Floeberg( boolean isS3Based) {
        this.isS3Based = isS3Based;

        if(isS3Based) {
            conf = new Configuration();
            conf.set("fs.s3a.access.key", AWSManager.getInstance().getAwsKey());
            conf.set("fs.s3a.secret.key", AWSManager.getInstance().getAwsSecret());
            conf.set("fs.s3a.endpoint", "s3.eu-west-1.amazonaws.com");



            conf.set("fs.s3a.fast.upload", "true");
            conf.set("fs.s3a.fast.upload.buffer", "disk");
            conf.set("fs.s3a.buffer.dir", "/tmp/hadoop");
            //Make sure that fs.s3a.connection.maximum is at least larger than fs.s3a.threads.max.
            conf.set("fs.s3a.threads.max", "10");
            conf.set("fs.s3a.connection.maximum", "20");

            locationOfTable  = DataLakeConfiguration.getInstance().getString("s3.test.bucket.uri" );
        }
        else {
            conf = new Configuration();
        }
        hTables = new HadoopTables(conf);
    }

    public Table setupCompanyTable() {
        companySchema = Company.getIcebergSchema();
        HashMap<String, String> tableProps = new HashMap<String, String>(0);

/*
        PartitionSpec pSpec =  PartitionSpec.builderFor(companySchema)
                .identity("company_type")    // THe field on which to create Partitions
                .build();
*/
        PartitionSpec pSpec = PartitionSpec.unpartitioned();

        Table tbl=  hTables.create(companySchema, pSpec, tableProps, locationOfTable);

        // check if any default properties are available ..
        if( !tbl.properties().isEmpty()) {
            tbl.properties().forEach((k, v) -> {
                System.out.println("key: " + k + ", value: " + v);
            });
        }
        else {
            System.out.println(
                    "No Table Properties defined as yet."
            );
        }
        return tbl;

    }

    public Table loadTableMetadata() {
        return hTables.load(locationOfTable);
    }

    public SparkSession getSparkSession() {
        SparkSession spark = SparkSession.builder()
                .appName("Spark Data")
                .master("local[*]")
                .config("spark.driver.memory","100m")
                .config("spark.eventLog.enabled", "true")
                .config("spark.eventLog.dir", "target/logs")
//                .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
//                .config("fs.s3a.endpoint", "s3.eu-west-1.amazonaws.com")
                .getOrCreate();

        if(this.isS3Based) {
            spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", AWSManager.getInstance().getAwsKey());
            spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", AWSManager.getInstance().getAwsSecret());
            spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-west-1.amazonaws.com");
            //Make sure that fs.s3a.connection.maximum is at least larger than fs.s3a.threads.max.
            spark.sparkContext().hadoopConfiguration().set("fs.s3a.threads.max", "20");
            spark.sparkContext().hadoopConfiguration().set("fs.s3a.connection.maximum", "40");

        }
        return spark;
    }

    public Dataset<Company>getTestData(int numberOfRows){
        return getSparkSession().createDataset(Company.createNRandomTestData(numberOfRows), Company.getEncoder());
    }

    public Dataset<Row> getTestDataAsRows(int numOfRows) {

        return getSparkSession().createDataFrame(Company.createNRandomTestDataOfRows(numOfRows),
                                                 Company.getSparkSchema());
    }
}
