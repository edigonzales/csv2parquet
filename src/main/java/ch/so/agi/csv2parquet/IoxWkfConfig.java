package ch.so.agi.csv2parquet;

public class IoxWkfConfig {
    private IoxWkfConfig() {}
    // Aus iox-wkf, damit die Reader und Validatoren konfiguriert
    // werden können.
    public final static String FILE_DIR="%CSV_DIR";
    private final static String PREFIX="ch.interlis.ioxwkf.dbtools";
    public final static String SETTING_MODELNAMES=PREFIX+".modelNames";
    public final static String SETTING_ILIDIRS=PREFIX+".settingIliDirs";
    public final static String SETTING_ILIDIRS_DEFAULT="https://geo.so.ch/models;http://models.interlis.ch/;";
    public final static String SETTING_FIRSTLINE=PREFIX+".firstLine";
    public final static String SETTING_FIRSTLINE_AS_HEADER="header";
    public final static String SETTING_FIRSTLINE_AS_VALUE="data";
    public final static String SETTING_VALUEDELIMITER=PREFIX+".valueDelimiter";
    public final static Character SETTING_VALUEDELIMITER_DEFAULT=null;
    public final static String SETTING_VALUESEPARATOR=PREFIX+".valueSeparator";
    public final static char SETTING_VALUESEPARATOR_DEFAULT=';';
    public final static int SETTING_SRSCODE_DEFAULT=2056;
    public final static String SETTING_DATEFORMAT=PREFIX+".dateFormat";
    public final static String SETTING_TIMEFORMAT=PREFIX+".timeFormat";
    public final static String SETTING_TIMESTAMPFORMAT=PREFIX+".timeStampFormat";
    public final static String SETTING_DEFAULTFORMAT_DATE="yyyy-MM-dd";
    public final static String SETTING_DEFAULTFORMAT_TIME="HH:mm:ss";
    public final static String SETTING_DEFAULTFORMAT_TIMESTAMP="yyyy-MM-dd'T'HH:mm:ss.SSS";
    public final static String SETTING_BATCHSIZE=PREFIX+".batchSize";
    public final static int SETTING_BATCHSIZE_DEFAULT=5000;
    
    // "Human readable", damit das config.ini file von Menschen verwendet
    // werden kann.
    
    public final static String INI_SECTION_PARAMETER = "PARAMETER";
    public final static String INI_FIRSTLINE_IS_HEADER = "firstLineIsHeader";
    public final static String INI_VALUEDELIMITER = "valueDelimiter";
    public final static String INI_VALUESEPARATOR = "valueSeparator";
    public final static String INI_ENCODING = "encoding";
    public final static String INI_MODELS = "models";
    public final static String SETTING_TRANSFERDESCRIPTION = "transferDescription";
    public final static String INI_META_TITLE = "title";
    public final static String INI_META_DESCRIPTION = "description";

}