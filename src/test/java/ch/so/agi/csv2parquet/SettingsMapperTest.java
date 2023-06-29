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
    public void settingsMapper_single_resource_no_id_set_Ok() throws Exception {
        // Prepare
        Path configPath = Paths.get("src/test/data/bewilligte_erdwaermeanlagen/ch.so.afu.bewilligte_erdwaermeanlagen.toml");
        
        // Run
        Settings settings = SettingsMapper.run(configPath.toFile(), null);

        // Validate
        assertEquals("header", settings.getValue(IoxWkfConfig.SETTING_FIRSTLINE));
        assertEquals(";", settings.getValue(IoxWkfConfig.SETTING_VALUESEPARATOR));
        assertEquals("UTF-8", settings.getValue(CsvReader.ENCODING));
        assertEquals("SO_AFU_Bewilligte_Erdwaermeanlagen_20230616", settings.getValue(Validator.SETTING_MODELNAMES));
    }
    
    @Test
    public void settingsMapper_single_resource_id_set_Ok() throws Exception {
        // Prepare
        Path configPath = Paths.get("src/test/data/bewilligte_erdwaermeanlagen/ch.so.afu.bewilligte_erdwaermeanlagen.toml");
        
        // Run
        Settings settings = SettingsMapper.run(configPath.toFile(), "ch.so.afu.bewilligte_erdwaermeanlagen");

        // Validate
        assertEquals("header", settings.getValue(IoxWkfConfig.SETTING_FIRSTLINE));
        assertEquals(";", settings.getValue(IoxWkfConfig.SETTING_VALUESEPARATOR));
        assertEquals("UTF-8", settings.getValue(CsvReader.ENCODING));
        assertEquals("SO_AFU_Bewilligte_Erdwaermeanlagen_20230616", settings.getValue(Validator.SETTING_MODELNAMES));
    }
    
    @Test
    public void settingsMapper_multiple_resources_id_set_Ok() throws Exception {
        // Prepare
        Path configPath = Paths.get("src/test/data/amtliche_vermessung_statistik/ch.so.agi.amtliche_vermessung_statistik.toml");
        
        // Run
        Settings settings = SettingsMapper.run(configPath.toFile(), "ch.so.agi.amtliche_vermessung_statistik.umsatz");

        // Validate
        assertEquals("header", settings.getValue(IoxWkfConfig.SETTING_FIRSTLINE));
        assertEquals(null, settings.getValue(IoxWkfConfig.SETTING_VALUESEPARATOR));
        assertEquals("SO_AGI_Amtliche_Vermessung_Statistik_Umsatz_20230625", settings.getValue(Validator.SETTING_MODELNAMES));
    }

}