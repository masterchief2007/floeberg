package com.mw.commons;


import org.apache.log4j.Logger;

public class AWSManager {
    private static AWSManager _instance = null;

    private final Logger logger = Logger.getLogger(AWSManager.class);


    private String awsKey = null;
    private String awsSecret= null;

    public static AWSManager getInstance() {
        if(_instance == null) {
            synchronized (AWSManager.class){
                if(_instance == null){
                    _instance = new AWSManager();
                }
            }
        }
        return _instance;
    }

    private AWSManager() {
        // Read Configuration
        String awsEnv= DataLakeConfiguration.getInstance().getString("awsenv");
        awsKey= DataLakeConfiguration.getInstance().getString("awskey");
        awsSecret= DataLakeConfiguration.getInstance().getString("awssecret");
        logger.debug("read the AWS keys...  for env " +  awsEnv);
    }

    public String getAwsKey(){
        return awsKey;
    }

    public String getAwsSecret() {
        return awsSecret;
    }
}
