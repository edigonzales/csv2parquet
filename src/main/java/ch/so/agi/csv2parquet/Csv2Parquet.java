package ch.so.agi.csv2parquet;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FilenameUtils;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.logging.StdListener;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.iox_j.StartTransferEvent;
import ch.interlis.ioxwkf.parquet.ParquetWriter;

public class Csv2Parquet {
    
    public boolean run(Path csvPath, Path outputPath, Settings config) throws IoxException {
        
        System.out.println(config);
        String csvBaseName = FilenameUtils.getBaseName(csvPath.getFileName().toString());
        
        // Weil erstes Element null-Werte hat, fehlen Attribute.
        // was liefert CSV-Reader genau?
        // Bekommt man irgendwie "Schema"? Falls ja: String-only Schema für Writer.
        ParquetWriter writer = new ParquetWriter(Paths.get(outputPath.toString(), csvBaseName + ".parquet").toFile());
        
        CsvReader reader = new CsvReader(csvPath.toFile());
        
        boolean firstLineIsHeader = false;
        if(config.getValue(IoxWkfConfig.SETTING_FIRSTLINE) != null) {
            firstLineIsHeader=config.getValue(IoxWkfConfig.SETTING_FIRSTLINE).equals(IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
        }
        reader.setFirstLineIsHeader(firstLineIsHeader);
        EhiLogger.traceState("first line is "+(firstLineIsHeader?"header":"data"));

        String valueDelimiter=config.getValue(IoxWkfConfig.SETTING_VALUEDELIMITER);
        if(valueDelimiter != null) {
            reader.setValueDelimiter(valueDelimiter.charAt(0));
            EhiLogger.traceState("valueDelimiter <"+valueDelimiter+">.");
        }
        String valueSeparator=config.getValue(IoxWkfConfig.SETTING_VALUESEPARATOR);
        if(valueSeparator != null) {
            reader.setValueSeparator(valueSeparator.charAt(0));
            EhiLogger.traceState("valueSeparator <"+valueSeparator+">.");
        }

        IoxEvent event = reader.read();
        while (event instanceof IoxEvent) {
            event = reader.read();
            if (event instanceof ObjectEvent) {
                                
                ObjectEvent iomObjEvent = (ObjectEvent) event;
                IomObject iomObj = iomObjEvent.getIomObject();
                
                writer.write(event);

    
//                System.out.println(iomObj);
//                System.out.println("ölpreis:"+iomObj.getattrvalue("durchschnittlicher_oelpreis_pro_1000_liter"));
            }
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

        return false;
    }
}
