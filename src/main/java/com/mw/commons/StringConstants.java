
package com.mw.commons;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Tom Cuong Ho
 */
public class StringConstants {
    public static String NEW_LINE_STR = "\n";
    public static String HYPHEN_STR = "-";
    public static String UNDERSCORE_STR = "_";
    public static String BOOLEAN_TRUE_STR = "t";
    public static String PERIOD_STR = ".";
    public static String COMMA_STR = ",";
    public static String COLON_STR = ":";
    public static String SEMICOLON_STR = ";";
    public static String EQUAL_STR = "=";
    public static String QUESTION_MARK_STR = "?";
    public static String QUESTION_MARK_ESCAPED_STR = "\\?";
    public static String PIPE_STR = "|";
    public static String PIPE_ESCAPED_STR = "\\|";
    public static String AMPERSAND_STR = "&";
    public static String FORWARD_SLASH_STR = "/";
    public static String OPEN_SQUARE_BRACKET_STR = "[";
    public static String OPEN_CURLY_BRACKET_STR = "{";
    public static String EMTY_STR = "";
    public static String TWO_DOUBLE_QUOTE_STR = "\"\"";
    public static String WHITE_SPACE_STR = " ";
    public static String WILDCARD_STR = "*";
    public static String SOLR_GET_ALL_STR = "*:*";
    public static String GMT_TIMEZONE = "GMT";
    public static String DEFAULT_CHARSET = "UTF-8";
    public static Charset DEFAULT_CHARSET_OBJ = Charset.forName(DEFAULT_CHARSET);
    public static String DEFAULT_VALUE_SEPARATOR = "=;==";
    public static String KEY_VALUE_SEPARATOR = ":";
    

    public static String COORDINATE_DEFAULT_VALUE = "0,0";

    public static final Charset UTF8_CHARSET = Charset.forName("UTF-8");


   public static String getUTF8String(String str) {
       if (StringUtils.isNotBlank(str)) {
           str = new String(str.getBytes(UTF8_CHARSET), UTF8_CHARSET);
       }
       return str;
   } 
   public static List<String> getUTF8StringList(List<String> strs) {
       List<String> retVal = null;
       if (strs != null && strs.size() > 0) {
           retVal = new ArrayList<String>(strs.size());
           for (String str : strs) {
               retVal.add(getUTF8String(str));
           }
       }
       return retVal;
   }
}
