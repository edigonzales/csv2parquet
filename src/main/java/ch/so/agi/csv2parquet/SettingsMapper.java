package ch.so.agi.csv2parquet;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.interlis2.validator.Validator;
import org.tomlj.Toml;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlTable;

import ch.ehi.basics.settings.Settings;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iox_j.inifile.IniFileReader;
import ch.interlis.iox_j.validator.ValidationConfig;

public class SettingsMapper {

    public static Settings run(File configFile, String identifier) throws IOException {
        Settings settings = new Settings();        

        TomlParseResult tomlContent = Toml.parse(configFile.toPath());
        
         
         Set<String> keySet = tomlContent.dottedKeySet();
         for (String foo : keySet) {
             System.out.println("**"+foo);
             
             
             
             
         }
          

               
        
        if (identifier == null) {
            // ???
            // Was machen?
            
            
            
        } else {
            
        }
        
        String parentIdentifier = identifier.substring(0, identifier.lastIndexOf("."));

        TomlTable parentTable = tomlContent.getTable(parentIdentifier);
        if (parentTable != null) {
            // Theme title von parent
        } else {
            // Theme title = resource title
        } 
        
        // Welche Kombi muss/will ich im CLI hier unterstützen?
        
        
        
        TomlTable resourceTable = tomlContent.getTable(identifier);
        
        
        
        
//        Set<Entry<String, Object>> entrySet = tomlContent.dottedEntrySet();
//        for (Entry<String, Object> entry : entrySet) {
//            System.out.println(entry);
//        }
//        
//        Set<List<String>> pathSet = tomlContent.keyPathSet();
//        for (List<String> entry : pathSet) {
//            System.out.println(entry);
//        }
//        
//        
//        Map<String, Object> metaTomlMap = tomlContent.toMap();
//
//        System.out.println(metaTomlMap.size());
//
//        for (Map.Entry<String,Object> entry : metaTomlMap.entrySet()) {
//            System.out.println(entry.getKey());
//        }
        
//        ValidationConfig config = IniFileReader.readFile(configFile);
//
//        String firstLineIsHeader = config.getConfigValue(IoxWkfConfig.INI_SECTION_PARAMETER,
//                IoxWkfConfig.INI_FIRSTLINE_IS_HEADER);
//        settings.setValue(IoxWkfConfig.SETTING_FIRSTLINE,
//                Boolean.parseBoolean(firstLineIsHeader) ? IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER
//                        : IoxWkfConfig.INI_FIRSTLINE_IS_HEADER);
//
//        String valueDelimiter = config.getConfigValue(IoxWkfConfig.INI_SECTION_PARAMETER,
//                IoxWkfConfig.INI_VALUEDELIMITER);
//
//        if (valueDelimiter != null) {
//            settings.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, valueDelimiter.replace("\\", ""));
//        } 
//
//        String valueSeparator = config.getConfigValue(IoxWkfConfig.INI_SECTION_PARAMETER,
//                IoxWkfConfig.INI_VALUESEPARATOR);
//        if (valueSeparator != null) {
//            settings.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, valueSeparator.replace("\\", ""));
//        }
//
//        String models = config.getConfigValue(IoxWkfConfig.INI_SECTION_PARAMETER, IoxWkfConfig.INI_MODELS);
//        if (models != null) {
//            settings.setValue(Validator.SETTING_MODELNAMES, models);
//        } 
//
//        String encoding = config.getConfigValue(IoxWkfConfig.INI_SECTION_PARAMETER, IoxWkfConfig.INI_ENCODING);
//        if (encoding != null) {
//            settings.setValue(CsvReader.ENCODING, encoding);
//        }

        return settings;
    }
}
