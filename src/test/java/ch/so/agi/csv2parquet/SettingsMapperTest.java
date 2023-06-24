package ch.so.agi.csv2parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.interlis2.validator.Validator;
import org.junit.jupiter.api.Test;

import ch.ehi.basics.settings.Settings;
import ch.interlis.iom_j.csv.CsvReader;

public class SettingsMapperTest {

    @Test
    public void settingsMapper_Ok() throws IOException {
        // Prepare
        Path configPath = Paths.get("src/test/data/config.ini");
        
        // Run
        Settings settings = SettingsMapper.run(configPath.toFile());

        // Validate
        assertEquals("header", settings.getValue(IoxWkfConfig.SETTING_FIRSTLINE));
        assertEquals(";", settings.getValue(IoxWkfConfig.SETTING_VALUESEPARATOR));
        assertEquals("\"", settings.getValue(IoxWkfConfig.SETTING_VALUEDELIMITER));
        assertEquals("ISO-8859-1", settings.getValue(CsvReader.ENCODING));
        assertEquals("SO_HBA_Gebaeude_20230111", settings.getValue(Validator.SETTING_MODELNAMES));
    }
}