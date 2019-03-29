package com.masterchief.data;

//{ "company" :  {"id" :  "001", "company_type" : "software",  "name" :  "test name 1", "website":  "http://www.testname1.com", "date_created":  1553194982, "date_updated":  1553194982}}

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;


public class Company implements Serializable {

    private String id;

    // Attributes
    private String company_type;
    private String name;
    private String website;

    // Metadata
    private Long date_created;
    private Long date_updated;



    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCompany_type() {
        return company_type;
    }

    public void setCompany_type(String company_type) {
        this.company_type = company_type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getWebsite() {
        return website;
    }

    public void setWebsite(String website) {
        this.website = website;
    }

    public Long getDate_created() {
        return date_created;
    }

    public void setDate_created(Long date_created) {
        this.date_created = date_created;
    }

    public Long getDate_updated() {
        return date_updated;
    }

    public void setDate_updated(Long date_updated) {
        this.date_updated = date_updated;
    }



    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("id: ");
        sb.append(id)
            .append(", name: ")
            .append(name)
            .append(", website: ")
            .append(website);

        return sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Company company = (Company) o;

        return new org.apache.commons.lang3.builder.EqualsBuilder()
                .append(id, company.id)
                .isEquals();
    }

    public static Company createRandomTestData(String testId) {
        Company c = new Company();
        c.setId(testId);

        c.setName(testId + "-company name");
        c.setCompany_type("test");
        c.setWebsite("http://www.test" + testId + ".com");

        Long time = new Long(System.currentTimeMillis());
        c.setDate_created(time);
        c.setDate_updated(0L);

        return c;
    }

    public static List<Company> createNRandomTestData(int n) {
        int minId = 10000, rangeMultiplier= 10000;

        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .appendLiteral("test-")
                .appendValue(ChronoField.YEAR)
                .appendLiteral("-")
                .appendValue(ChronoField.MONTH_OF_YEAR)
                .appendLiteral("-")
                .appendValue(ChronoField.DAY_OF_MONTH)
                .appendLiteral("_")
                .appendValue(ChronoField.MINUTE_OF_DAY)
                .toFormatter();

        String prefix= LocalDateTime.now().format(formatter);

        List<Company> listOfCompanies = new ArrayList<Company>();

        for( int i = 1; i <= n ; i++) {
            long randomId= (long)Math.ceil(((Math.random()* (i*rangeMultiplier)) + minId));
            listOfCompanies.add(Company.createRandomTestData(prefix + randomId));
        }

        return listOfCompanies;
    }

    public static Schema getIcebergSchema() {
/*
        // Attributes
        private String company_type;
        private String name;
        private String website;

        // Metadata
        private Long date_created;
        private Long date_updated;
*/

        List<Types.NestedField> fields = new ArrayList<>();

        fields.add(Types.NestedField.required(1, "id", Types.StringType.get()));
        fields.add(Types.NestedField.optional(2, "company_type", Types.StringType.get()));
        fields.add(Types.NestedField.optional(3, "name", Types.StringType.get()));
        fields.add(Types.NestedField.optional(4, "website", Types.StringType.get()));

        fields.add(Types.NestedField.optional(5, "date_created", Types.LongType.get()));
        fields.add(Types.NestedField.optional(6, "date_updated", Types.LongType.get()));

        return new Schema( fields );
    }
}
