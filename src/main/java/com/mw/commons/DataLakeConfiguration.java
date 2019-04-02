package com.mw.commons;

import org.apache.commons.configuration.*;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.configuration.tree.NodeCombiner;
import org.apache.commons.configuration.tree.UnionCombiner;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class DataLakeConfiguration {

    private String configPathFileName = ConfigConstants.DEFAULT_CONFIG_FILE_NAME;
    private CombinedConfiguration combinedConfiguration;
    private static DataLakeConfiguration self = new DataLakeConfiguration();
    private static boolean failed = false;

    public static DataLakeConfiguration getInstance() {
        if (failed) {
            return null;
        } else {
            return self;
        }
    }

    private DataLakeConfiguration() {
        this.init();
    }

    public void init() {
        DefaultConfigurationBuilder builder = new DefaultConfigurationBuilder();
        if (System.getProperty("config.file") != null) {
            this.configPathFileName = System.getProperty("config.file").trim();
        }
        System.out.println("configPathFileName=" + this.configPathFileName);
        //preventing WARN message for config-optional=true attribute in config.xml
        Level level = LogManager.getLogger("org.apache.commons.configuration").getLevel();
        LogManager.getLogger("org.apache.commons.configuration").setLevel(Level.ERROR);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL configResource = classLoader.getResource(configPathFileName);
        builder.setURL(configResource);
        try {
            this.combinedConfiguration = builder.getConfiguration(true);
        } catch (ConfigurationException ex) {
            System.err.println(String.format("Error initializing configuration: %s", ex));;
        }
        LogManager.getLogger("org.apache.commons.configuration").setLevel(level);
    }

    public void init(String configFile) {
        DefaultConfigurationBuilder builder = new DefaultConfigurationBuilder();
        if (configFile != null && !configFile.trim().isEmpty()) {
            this.configPathFileName = configFile.trim();
        }
        //preventing WARN message for config-optional=true attribute in config.xml
        Level level = LogManager.getLogger("org.apache.commons.configuration").getLevel();
        LogManager.getLogger("org.apache.commons.configuration").setLevel(Level.ERROR);
        builder.setFile(new File(this.configPathFileName));
        System.out.println("configPathFileName=" + this.configPathFileName);
        try {
            this.combinedConfiguration = builder.getConfiguration(true);
        } catch (ConfigurationException ex) {
            System.err.println(String.format("Error initilizing configuration: %s", ex));;
        }
        LogManager.getLogger("org.apache.commons.configuration").setLevel(level);
    }

    public void init(URL configFile) {
        DefaultConfigurationBuilder builder = new DefaultConfigurationBuilder();

        //preventing WARN message for config-optional=true attribute in config.xml
        Level level = LogManager.getLogger("org.apache.commons.configuration").getLevel();
        LogManager.getLogger("org.apache.commons.configuration").setLevel(Level.ERROR);
        builder.setURL(configFile);
        System.out.println("configPathFileName=" + this.configPathFileName);
        try {
            this.combinedConfiguration = builder.getConfiguration(true);
        } catch (ConfigurationException ex) {
            System.err.println(String.format("Error initilizing configuration: %s", ex));;
        }
        LogManager.getLogger("org.apache.commons.configuration").setLevel(level);
    }

    public CombinedConfiguration getCConfiguration() {
        return this.combinedConfiguration;
    }

    public Configuration getConfiguration(String configName) {
        return this.getCConfiguration().getConfiguration(configName);
    }

    /**
     * Configure logging from the log configuration file specified by
     * "config-name" attribute and periodically check if the file has been
     * created or modified according to a delay specified in reloadingStrategy
     * tag of config.xml.
     *
     * @param configName
     */
    public synchronized void configureLogging(String configName) {
        long delay = 0;
        try {
            delay = ((FileChangedReloadingStrategy) ((PropertiesConfiguration) this.getCConfiguration().getConfiguration(configName)).getReloadingStrategy()).getRefreshDelay();
        } catch (Exception ex) {
        }
        if (delay > 0) {
            PropertyConfigurator.configureAndWatch(
                    ((PropertiesConfiguration) this.getCConfiguration().getConfiguration(configName)).getFileName(), delay);
        } else {
            this.configureLoggingFromProperties();
        }
    }

    public synchronized void configureLogging() {
        this.configureLogging(ConfigConstants.DEFAULT_LOG_PROPERTY_CONFIG_NAME);
    }

    /**
     * Creates a separate log file for the executable class.
     *
     * @param clazz class with main() method
     */
    public void configureLogging(Class clazz) {
        if (StringUtils.isNotBlank(this.getCConfiguration().getString("projectLogDir"))) {
            this.getCConfiguration().setProperty("log4j.appender.R.File",
                    this.getCConfiguration().getString("projectLogDir") + "/" + clazz.getSimpleName() + ".log");
        }
        DataLakeConfiguration.getInstance().configureLogging();
    }
    /**
     * Configure logging from CombinedConfiguration without checking if the file
     * has been created or modified.
     *
     */
    public synchronized void configureLoggingFromProperties() {
        PropertyConfigurator.configure(ConfigurationConverter.getProperties(this.getCConfiguration()));
    }

    public String getString(String key, String defaultValue) {
        String retVal = getString(key);
        if (StringUtils.isBlank(retVal)) {
            retVal = defaultValue;
        }

        return retVal;

    }

    public String getString(String key) {
        String retVal = null;
        String[] values = combinedConfiguration.getStringArray(key);
        if (values != null && values.length != 0) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < values.length; i++) {
                if (sb.length() > 0) {
                    sb.append(StringConstants.COMMA_STR);
                }
                sb.append(values[i]);
            }
            retVal = sb.toString();
        }

        return retVal;

    }

    /**
     *
     * @param configNameAttr
     * @return null if CONFIG_FILE_NAME doesn't contain a node with value
     * configName in attribute "config-name" or file in the attribute "fileName"
     * doesn't exist
     */
    public String getExternalConfigFilePath(String configNameAttr) {
        FileConfiguration conf = (FileConfiguration) this.getCConfiguration().getConfiguration(configNameAttr);
        if (conf != null && new File(conf.getFileName()).exists()) {
            return conf.getFileName();
        } else {
            return null;
        }
    }
    /**
     *
     * @param configNameAttr
     * @param combinedListNodes
     * @return
     */
    public CombinedConfiguration getXmlCConfiguration(List<String> configNameAttr, String... combinedListNodes) {
        NodeCombiner combiner = new UnionCombiner();
        for (String listNode : combinedListNodes) {
            // mark listNode as list node(do not merge listNode into one node)
            combiner.addListNode(listNode);
        }
        CombinedConfiguration combinedXmlConfiguration = new CombinedConfiguration(combiner);

        for (String configName : configNameAttr) {
            Configuration xmlConfiguration = this.getCConfiguration().getConfiguration(configName);
            if (xmlConfiguration instanceof XMLConfiguration) {
                combinedXmlConfiguration.addConfiguration((XMLConfiguration) xmlConfiguration, configName);
            }
        }
        return combinedXmlConfiguration;
    }
    /**
     * [prefix].[prop] -> [prop].
     *
     * @param prefix [prefix]
     * @return all properties with provided prefix
     */
    public Properties getProperties(String prefix) {
        Properties props = new Properties();
        Iterator<String> iter = this.getCConfiguration().getKeys(prefix);
        while (iter.hasNext()) {
            String key = iter.next();
            String prop = key.substring(prefix.length());
            props.put(prop, this.getString(key));
        }
        return props;
    }

}
