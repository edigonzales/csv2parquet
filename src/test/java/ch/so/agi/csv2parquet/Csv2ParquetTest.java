package ch.so.agi.csv2parquet;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

public class Csv2ParquetTest {
    @Test
    public void no_model_set_Ok() {
        // Prepare
        CsvSettings settings = new CsvSettings();
        Path csvPath = Paths.get("src/test/data/bewilligte_erdwaermeanlagen.csv");
        Path outputPath = Paths.get("build/");
        
        // Run
        Csv2Parquet csv2parquet = new Csv2Parquet();
        csv2parquet.run(csvPath, outputPath, settings);
        
        // Validate
    }
}
