package ch.so.agi.csv2parquet;

public class CsvConfig {
    public final static String SETTING_SECTION_PARAMETER = "PARAMETER";
    public final static String SETTING_FIRSTLINE_IS_HEADER = "firstLineIsHeader";
    public final static String SETTING_FIRSTLINE_AS_HEADER = "header";
    public final static String SETTING_FIRSTLINE_AS_VALUE = "data";
    public final static String SETTING_VALUEDELIMITER = "valueDelimiter";
    public final static String SETTING_VALUESEPARATOR = "valueSeparator";
    public final static String SETTING_ENCODING = "encoding";
    public final static String SETTING_MODELS = "models";
    
    public final static String SETTING_FIRSTLINE_IS_HEADER_DEFAULT = "true";
    public final static Character SETTING_VALUEDELIMITER_DEFAULT = ',';
    public final static Character SETTING_VALUESEPARATOR_DEFAULT = '\"';
    public final static String SETTING_ENCODING_DEFAULT = "UTF-8";
}
