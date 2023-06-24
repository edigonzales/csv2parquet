package ch.so.agi.csv2parquet;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.interlis2.validator.Validator;
import org.junit.jupiter.api.Test;

import ch.ehi.basics.settings.Settings;

public class CsvValidatorTest {
   
    @Test
    public void validate_Ok() throws Exception {
        // Prepare
        Settings settings = new Settings();
        settings.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
        settings.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, null);
        settings.setValue(Validator.SETTING_MODELNAMES, "SO_AFU_Bewilligte_Erdwaermeanlagen_20230616");

        Path csvPath = Paths.get("src/test/data/bewilligte_erdwaermeanlagen_excel_export.csv");
        
        // Run
        String[] csvFiles = { csvPath.toFile().getAbsolutePath() };
        boolean validationOk = new CsvValidator().validate(csvFiles, settings);

        // Validate
        assertTrue(validationOk);
    }

    @Test
    public void validate_Fail() throws Exception {
        // Prepare
        Settings settings = new Settings();
        settings.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
        settings.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, null);
        settings.setValue(Validator.SETTING_MODELNAMES, "SO_AFU_Bewilligte_Erdwaermeanlagen_20230616");

        Path csvPath = Paths.get("src/test/data/bewilligte_erdwaermeanlagen_validation_error.csv");
        
        // Run
        String[] csvFiles = { csvPath.toFile().getAbsolutePath() };
        boolean validationOk = new CsvValidator().validate(csvFiles, settings);

        // Validate
        assertFalse(validationOk);
    }

}
