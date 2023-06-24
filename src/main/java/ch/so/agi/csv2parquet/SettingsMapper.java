package ch.so.agi.csv2parquet;

import java.io.File;
import java.io.IOException;

import org.interlis2.validator.Validator;

import ch.ehi.basics.settings.Settings;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iox_j.inifile.IniFileReader;
import ch.interlis.iox_j.validator.ValidationConfig;

public class SettingsMapper {

    public static Settings run(File configFile) throws IOException {
        Settings settings = new Settings();
        ValidationConfig config = IniFileReader.readFile(configFile);

        String firstLineIsHeader = config.getConfigValue(IoxWkfConfig.INI_SECTION_PARAMETER,
                IoxWkfConfig.INI_FIRSTLINE_IS_HEADER);
        settings.setValue(IoxWkfConfig.SETTING_FIRSTLINE,
                Boolean.parseBoolean(firstLineIsHeader) ? IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER
                        : IoxWkfConfig.INI_FIRSTLINE_IS_HEADER);

        String valueDelimiter = config.getConfigValue(IoxWkfConfig.INI_SECTION_PARAMETER,
                IoxWkfConfig.INI_VALUEDELIMITER);

        if (valueDelimiter != null) {
            settings.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, valueDelimiter.replace("\\", ""));
        } 

        String valueSeparator = config.getConfigValue(IoxWkfConfig.INI_SECTION_PARAMETER,
                IoxWkfConfig.INI_VALUESEPARATOR);
        if (valueSeparator != null) {
            settings.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, valueSeparator.replace("\\", ""));
        }

        String models = config.getConfigValue(IoxWkfConfig.INI_SECTION_PARAMETER, IoxWkfConfig.INI_MODELS);
        if (models != null) {
            settings.setValue(Validator.SETTING_MODELNAMES, models);
        } 

        String encoding = config.getConfigValue(IoxWkfConfig.INI_SECTION_PARAMETER, IoxWkfConfig.INI_ENCODING);
        if (encoding != null) {
            settings.setValue(CsvReader.ENCODING, encoding);
        }

        return settings;
    }
}
