package ch.so.agi.csv2parquet;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;

public class Csv2ParquetTest {
    @Test
    public void default_config_and_no_model_set_Ok() throws IoxException {
        // Prepare
        Settings settings = new Settings();
        settings.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
        settings.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, "\"");            
        settings.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, ";");            

        
        Path csvPath = Paths.get("src/test/data/bewilligte_erdwaermeanlagen.csv");
        Path outputPath = Paths.get("build/");
        
        // Run
        Csv2Parquet csv2parquet = new Csv2Parquet();
        csv2parquet.run(csvPath, outputPath, settings);
        
        // Validate
    }
}
