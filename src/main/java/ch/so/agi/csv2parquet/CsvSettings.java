package ch.so.agi.csv2parquet;

import ch.ehi.basics.settings.Settings;
import ch.interlis.iom_j.csv.CsvReader;

public class CsvSettings extends Settings {
    
    public CsvSettings() {
        super();
        this.setValue(CsvConfig.SETTING_FIRSTLINE_IS_HEADER, CsvConfig.SETTING_FIRSTLINE_IS_HEADER_DEFAULT);
        this.setValue(CsvConfig.SETTING_VALUEDELIMITER, CsvConfig.SETTING_VALUEDELIMITER_DEFAULT.toString());
        this.setValue(CsvConfig.SETTING_VALUESEPARATOR, CsvConfig.SETTING_VALUESEPARATOR_DEFAULT.toString());
        this.setValue(CsvReader.ENCODING, CsvConfig.SETTING_ENCODING_DEFAULT);        
    }
}
