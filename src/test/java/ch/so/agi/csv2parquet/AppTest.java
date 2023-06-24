package ch.so.agi.csv2parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;

import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;

class AppTest {
    private static final Configuration testConf = new Configuration();

    @Test 
    public void validation_Fail() {
        // Prepare
        App app = new App();
        CommandLine cmd = new CommandLine(app);

        StringWriter sw = new StringWriter();
        cmd.setOut(new PrintWriter(sw));

        // Run
        int exitCode = cmd.execute("--config=src/test/data/config_erdwaermeanlagen.ini", "--input=src/test/data/bewilligte_erdwaermeanlagen_validation_error.csv");

        // Validate
        assertEquals(2, exitCode);
    }
    
    @Test 
    public void convert_model_set_Ok() throws IOException {
        // Prepare
        String csvBaseName = "bewilligte_erdwaermeanlagen_excel_export";
        Path csvPath = Paths.get("src/test/data/", csvBaseName + ".csv");
        Path outputPath = Paths.get("build/test/data/app_convert_Ok/");
        outputPath.toFile().mkdirs();
        
        App app = new App();
        CommandLine cmd = new CommandLine(app);

        StringWriter sw = new StringWriter();
        cmd.setOut(new PrintWriter(sw));

        // Run
        int exitCode = cmd.execute("--config=src/test/data/config_erdwaermeanlagen.ini",
                "--input="+csvPath.toString(),
                "--output="+outputPath.toString());
        
        
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
        assertEquals(1991, firstRecord.get("jahr"));
        assertEquals(null, firstRecord.get("internet_clicks_durchschnitt_pro_monat"));        
        assertEquals(2021, lastRecord.get("jahr"));
        assertEquals(999, lastRecord.get("internet_clicks_durchschnitt_pro_monat"));

        assertEquals(0, exitCode);
    }
}
