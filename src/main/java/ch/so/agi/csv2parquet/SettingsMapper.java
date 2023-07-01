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
            
//            // TODO wird erst interessant, wenn wir neben tool-config-Daten auch Metadatenauslesen.
//            
//            // In der Methode wird also immer der identifier (oder falls null irgendeins) verwendet.
//            // Falls ein parentTable vorhanden ist, muss man diesen noch behandeln. Gewisse Teile
//            // werden überschrieben? / verschoben?
//            if (parentTable != null) {
//                // Theme title von parent
//                // ...
//                String title = parentTable.getString(IoxWkfConfig.INI_META_TITLE);
//                if (title != null) {
//                    settings.setValue(IoxWkfConfig.INI_META_PARENT_TITLE, title);
//                }
//                
//                String description = parentTable.getString(IoxWkfConfig.INI_META_DESCRIPTION);
//                if (description != null) {
//                    settings.setValue(IoxWkfConfig.INI_META_PARENT_DESCRIPTION, description);
//                }
//            }
        }
        
//        if (settings.getValue(Validator.SETTING_MODELNAMES) != null) {
//            TransferDescription td = getTransferDescriptionFromModelName(settings.getValue(Validator.SETTING_MODELNAMES), configFile.getParentFile().toPath());
//            settings.setTransientObject(IoxWkfConfig.SETTING_TRANSFERDESCRIPTION, td);
//        }
//        
//        
//        System.out.println(settings);
        
//        TomlMapper tomlMapper = new TomlMapper();
//        tomlMapper.registerModule(new JavaTimeModule());
//        Map<String, Object> tomlMap = tomlMapper.readValue(configFile, Map.class);
//        System.out.println(tomlMap);

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
            
//            if (key.endsWith(IoxWkfConfig.INI_META_TITLE)) {
//                String title = tomlTable.getString(key);
//                if (title != null) {
//                    settings.setValue(IoxWkfConfig.INI_META_TITLE, title);
//                }
//            }
            
            //...
        }
    }
    
    private static TransferDescription getTransferDescriptionFromModelName(String iliModelName, Path additionalRepository) throws Ili2cException {
        IliManager manager = new IliManager();        
        String ilidirs = IoxWkfConfig.SETTING_ILIDIRS_DEFAULT + additionalRepository;
        String repositories[] = ilidirs.split(";");
        manager.setRepositories(repositories);
        ArrayList<String> modelNames = new ArrayList<String>();
        modelNames.add(iliModelName);
        Configuration config = manager.getConfig(modelNames, 2.3);
        TransferDescription td = Ili2c.runCompiler(config);

        if (td == null) {
            throw new IllegalArgumentException("INTERLIS compiler failed"); // TODO: can this be tested?
        }
        
        return td;
    }

    
}
