package ch.so.agi.csv2parquet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.interlis2.validator.Validator;
import org.tomlj.Toml;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlTable;

import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.Ili2c;
import ch.interlis.ili2c.Ili2cException;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ilirepository.IliManager;
import ch.interlis.iom_j.csv.CsvReader;

public class SettingsMapper {

    public static Settings run(File configFile, String identifier) throws IOException, Ili2cException {
        Settings settings = new Settings();

        TomlParseResult tomlContent = Toml.parse(configFile.toPath());
        Map<String,Object> tomlMap = tomlContent.toMap();
        
        // Falls kein Identifier gesetzt wurde, wird das ganze Toml-File
        // durchsucht und der erste Match verwendet.
        if (identifier == null) {            
            setSettingsFromTomlTable(tomlContent, settings);
        } else {
            String parentIdentifier = identifier.substring(0, identifier.lastIndexOf("."));
            TomlTable parentTable = (TomlTable) tomlMap.get(parentIdentifier);

            TomlTable resourceTable = (TomlTable) tomlMap.get(identifier);            
            if (resourceTable != null) {
                setSettingsFromTomlTable(resourceTable, settings);   
            }
            
        }
        
        return settings;
    }
    
    private static void setSettingsFromTomlTable(TomlTable tomlTable, Settings settings) {
        Set<String> keySet = tomlTable.dottedKeySet();

        for (String key : keySet) {
            if (key.endsWith(IoxWkfConfig.INI_FIRSTLINE_IS_HEADER)) {
                boolean firstLineIsHeader = tomlTable.getBoolean(key);
                settings.setValue(IoxWkfConfig.SETTING_FIRSTLINE,
                        firstLineIsHeader ? IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER
                                : IoxWkfConfig.INI_FIRSTLINE_IS_HEADER);
            }

            if (key.endsWith(IoxWkfConfig.INI_VALUEDELIMITER)) {
                String valueDelimiter = tomlTable.getString(key);
                if (valueDelimiter != null) {
                    settings.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, valueDelimiter.replace("\\", ""));
                }
            }

            if (key.endsWith(IoxWkfConfig.INI_VALUESEPARATOR)) {
                String valueSeparator = tomlTable.getString(key);
                if (valueSeparator != null) {
                    settings.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, valueSeparator.replace("\\", ""));
                }
            }

            if (key.endsWith(IoxWkfConfig.INI_MODELS)) {
                String models = tomlTable.getString(key);
                if (models != null) {
                    settings.setValue(Validator.SETTING_MODELNAMES, models);                    
                }
            }

            if (key.endsWith(IoxWkfConfig.INI_ENCODING)) {
                String encoding = tomlTable.getString(key);
                if (encoding != null) {
                    settings.setValue(CsvReader.ENCODING, encoding);
                }
            }            
        }
    }    
}
