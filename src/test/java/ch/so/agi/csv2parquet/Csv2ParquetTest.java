package ch.so.agi.csv2parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;

import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;

public class Csv2ParquetTest {
    private static final Configuration testConf = new Configuration();

    //TODO Tests:
    // - Datum aus CSV (im CSV-Reader gibts glaubs Logik)
    // - App-Tests wegen Config.ini?
    
    
    @Test
    public void default_config_and_no_model_set_Ok() throws IoxException, IOException {
        // Prepare
        Settings settings = new Settings();
        settings.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
        
        Path csvPath = Paths.get("src/test/data/bewilligte_erdwaermeanlagen_komma_anfuehrungszeichen.csv");
        Path outputPath = Paths.get("build/test/data/default_config_and_no_model_set_Ok/");
        outputPath.toFile().mkdirs();
        
        // Run
        Csv2Parquet csv2parquet = new Csv2Parquet();
        csv2parquet.run(csvPath, outputPath, settings);
        
        // Validate
        org.apache.hadoop.fs.Path resultFile = new org.apache.hadoop.fs.Path(Paths
                .get(outputPath.toString(), FilenameUtils.getBaseName(csvPath.toString()) + ".parquet").toString());
        ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(HadoopInputFile.fromPath(resultFile, testConf)).build();

        int recordCount = 0;
        GenericRecord arecord = reader.read();
        GenericRecord firstRecord = null;
        GenericRecord lastRecord = null;
        while(arecord != null) {
            if (recordCount == 0) {
                firstRecord = arecord;
            }
            recordCount++;
            
            lastRecord = arecord;
            arecord = reader.read();
        }
        
        assertEquals(31, recordCount);
        assertTrue(firstRecord.hasField("durchschnittlicher_oelpreis_pro_1000_liter"));
        assertTrue((firstRecord.get("durchschnittlicher_oelpreis_pro_1000_liter") == null));
        assertEquals(999, Integer.valueOf(lastRecord.get("internet_clicks_durchschnitt_pro_monat").toString()));
    }

    @Test
    public void custom_config_and_no_model_set_Ok() throws IoxException, IOException {
        // Prepare
        Settings settings = new Settings();
        settings.setValue(IoxWkfConfig.SETTING_FIRSTLINE, IoxWkfConfig.SETTING_FIRSTLINE_AS_HEADER);
        settings.setValue(IoxWkfConfig.SETTING_VALUEDELIMITER, "'");            
        settings.setValue(IoxWkfConfig.SETTING_VALUESEPARATOR, ";");            
        
        Path csvPath = Paths.get("src/test/data/bewilligte_erdwaermeanlagen_semikolon_hochkomma.csv");
        Path outputPath = Paths.get("build/test/data/custom_config_and_no_model_set_Ok/");
        outputPath.toFile().mkdirs();
        
        // Run
        Csv2Parquet csv2parquet = new Csv2Parquet();
        csv2parquet.run(csvPath, outputPath, settings);

        // Validate
        org.apache.hadoop.fs.Path resultFile = new org.apache.hadoop.fs.Path(Paths
                .get(outputPath.toString(), FilenameUtils.getBaseName(csvPath.toString()) + ".parquet").toString());
        ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(HadoopInputFile.fromPath(resultFile, testConf)).build();

        int recordCount = 0;
        GenericRecord arecord = reader.read();
        GenericRecord firstRecord = null;
        GenericRecord lastRecord = null;
        while(arecord != null) {
            if (recordCount == 0) {
                firstRecord = arecord;
            }
            recordCount++;
            
            lastRecord = arecord;
            arecord = reader.read();
        }
        
        assertEquals(31, recordCount);
        assertTrue(firstRecord.hasField("durchschnittlicher_oelpreis_pro_1000_liter"));
        assertTrue((firstRecord.get("durchschnittlicher_oelpreis_pro_1000_liter") == null));
        assertEquals(999, Integer.valueOf(lastRecord.get("internet_clicks_durchschnitt_pro_monat").toString()));
    }
}
