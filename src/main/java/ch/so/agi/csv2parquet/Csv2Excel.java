package ch.so.agi.csv2parquet;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.interlis2.validator.Validator;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.Ili2c;
import ch.interlis.ili2c.Ili2cException;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ilirepository.IliManager;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.ioxwkf.excel.ExcelWriter;
import ch.interlis.ioxwkf.excel.ExcelAttributeDescriptor;

public class Csv2Excel {
    
    public boolean run(Path csvPath, Path outputPath, Settings config)  {
        //EhiLogger.getInstance().setTraceFilter(false);
        
        String csvBaseName = FilenameUtils.getBaseName(csvPath.getFileName().toString());
        ExcelWriter writer = null;
        try {
            writer = new ExcelWriter(Paths.get(outputPath.toString(), csvBaseName + ".xlsx").toFile());
        } catch (IoxException e) {
            EhiLogger.logError(e);
            return true;
        }
        
        CsvReader reader = null;
        try {
            reader = new CsvReader(csvPath.toFile(), config); // config notwendig, wegen encoding, das im Reader gesetzt wird.
        } catch (IoxException e) {
            EhiLogger.logError(e);
            return true;
        }
        
        boolean firstLineIsHeader = true;
        if(config.getValue(IoxWkfConfig.SETTING_FIRSTLINE) != null) {
            firstLineIsHeader=config.getValue(IoxWkfConfig.SETTING_FIRSTLINE).equals(IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
        } 
        reader.setFirstLineIsHeader(firstLineIsHeader);
        EhiLogger.traceState("first line is "+(firstLineIsHeader?"header":"data"));

        String valueDelimiter=config.getValue(IoxWkfConfig.SETTING_VALUEDELIMITER);
        if(valueDelimiter != null) {
            reader.setValueDelimiter(valueDelimiter.charAt(0));
            EhiLogger.traceState("valueDelimiter <"+valueDelimiter+">.");
        } else {
            // Weil default=null ist, muss nichts gesetzt werden.
            EhiLogger.traceState("valueDelimiter <"+IoxWkfConfig.SETTING_VALUEDELIMITER_DEFAULT+">.");
        }

        String valueSeparator=config.getValue(IoxWkfConfig.SETTING_VALUESEPARATOR);
        if(valueSeparator != null) {
            reader.setValueSeparator(valueSeparator.charAt(0));
            EhiLogger.traceState("valueSeparator <"+valueSeparator+">.");
        } else {
            reader.setValueSeparator(IoxWkfConfig.SETTING_VALUESEPARATOR_DEFAULT);
            EhiLogger.traceState("valueSeparator <"+IoxWkfConfig.SETTING_VALUESEPARATOR_DEFAULT+">.");
        }
                
        if (config.getValue(Validator.SETTING_MODELNAMES) != null) {
            TransferDescription td = null;
            try {
                td = getTransferDescriptionFromModelName(config.getValue(Validator.SETTING_MODELNAMES), csvPath.getParent());
            } catch (Ili2cException e) {
                EhiLogger.logError(e);
                return true;
            }
            reader.setModel(td);
            writer.setModel(td);
        }
        
        String[] attrs = null;
        
        try {
            IoxEvent event = reader.read();

            while (event instanceof IoxEvent) {
                //event = reader.read();
                if (event instanceof ObjectEvent) {
                    if (attrs == null) {
                        attrs = reader.getAttributes();
                        
                        // Funktioniert, falls die Reihenfolge garantiert ist.
                        // Man muss die Attribute explizit setzen. Sonst kann 
                        // passieren, dass Attribute komplett fehlen, weil das
                        // erste Objekt analysiert wird und einige Attribute davon
                        // null sind und im IomObjekt nicht vorkommen.
                        // Und funktioniert nur, falls Header-Zeile vorhanden.
                        if (firstLineIsHeader && config.getValue(Validator.SETTING_MODELNAMES) == null) {
                            List<ExcelAttributeDescriptor> attrDescs = new ArrayList<>();
                            for(String attrName : attrs) {                        
                                ExcelAttributeDescriptor attrDesc = new ExcelAttributeDescriptor();
                                attrDesc.setAttributeName(attrName);
                                attrDesc.setBinding(String.class);
                                attrDescs.add(attrDesc);
                            }
                            writer.setAttributeDescriptors(attrDescs);                        
                        }
                    }
//                    
//                    ObjectEvent iomObjEvent = (ObjectEvent) event;
//                    IomObject iomObj = iomObjEvent.getIomObject();
//                    System.out.println(iomObj.toString());
                    
                    writer.write(event);
                }
                event = reader.read();
            }

            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());

            if (writer != null) {
                writer.close();
                writer = null;
            }
            
            if (reader != null) {
                reader.close();
                reader = null;
            }
        } catch (IoxException e) {
            EhiLogger.logError(e);
            return true;
        }
        return false;
    }
    
    private TransferDescription getTransferDescriptionFromModelName(String iliModelName, Path additionalRepository) throws Ili2cException {
        IliManager manager = new IliManager();        
        String ilidirs = IoxWkfConfig.SETTING_ILIDIRS_DEFAULT + additionalRepository;
        String repositories[] = ilidirs.split(";");
//        for (String repo : repositories) {
//            System.out.println(repo);            
//        }
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
