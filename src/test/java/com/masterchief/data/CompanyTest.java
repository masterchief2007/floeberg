package com.masterchief.data;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;


public class CompanyTest {

    @Test
    public void testRandomDataGeneration() {

        List<Company> companies = Company.createNRandomTestData(20) ;

        assertNotNull("Companies List was not created.", companies);
        assertEquals(20, companies.size());

        companies.stream().forEach(company -> {
            System.out.println( company);
        });
    }

    @Test
    public void testRandomDataGenerationUniqueness() {

        int testDataSize = 1000;
        List<Company> companies = Company.createNRandomTestData(testDataSize) ;

        assertNotNull("Companies List was not created.", companies);
        assertEquals(testDataSize, companies.size());
        HashMap<String, Boolean> found = new HashMap<String, Boolean>();

/*
        companies.stream().forEach(company -> {
            System.out.println( company);
        });
*/
        companies.stream().forEach(company -> {

            found.computeIfPresent(company.getId(), (k,v) -> {
                assertFalse("Duplicate key found." + k, true);
                return v;
            });

            found.putIfAbsent(company.getId(), true);
        });
    }


}
