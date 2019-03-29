package com.masterchief;

import com.masterchief.data.Company;
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
    private HadoopTables hTables ;
    Schema  companySchema;


    public Floeberg() {
        conf = new Configuration();
        hTables = new HadoopTables(conf);
    }

    public Table setupCompanyTable() {
        companySchema = Company.getIcebergSchema();
        HashMap<String, String> tableProps = new HashMap<String, String>(0);

        PartitionSpec pSpec =  PartitionSpec.builderFor(companySchema)
                .identity("company_type")    // THe field on which to create Partitions
                .build();

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
                .config("spark.driver.memory","8g")
                .config("spark.eventLog.enabled", "true")
                .config("spark.eventLog.dir", "target/logs")
                .getOrCreate();

        return spark;
    }

    public Dataset<Row>getTestData(int numberOfRows){
        return getSparkSession().createDataFrame(Company.createNRandomTestData(numberOfRows), Company.class);
    }
}
