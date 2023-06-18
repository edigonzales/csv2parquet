package ch.so.agi.csv2parquet;

import java.io.File;
import java.util.concurrent.Callable;

import ch.ehi.basics.settings.Settings;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iox_j.inifile.IniFileReader;
import ch.interlis.iox_j.validator.ValidationConfig;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
        name = "csv2parquet",
        description = "Convert csv files to parquet files.",
        //version = "ili2repo version 0.0.1",
        mixinStandardHelpOptions = true,
        
        headerHeading = "%n",
        //synopsisHeading = "%n",
        descriptionHeading = "%nDescription: ",
        parameterListHeading = "%nParameters:%n",
        optionListHeading = "%nOptions:%n"
      )
public class App implements Callable<Integer> {
    
    @Option(names = { "-c", "--config" }, required = false, description = "The configuration file.")
    File configFile;
    
    @Option(names = { "-i", "--input" }, required = true, description = "The input csv file.") 
    File csvFile;
    
    @Option(names = { "-o", "--output" }, required = false, description = "The output directory where the resulting parquet file is written to.") 
    File outputDir;

    @Override
    public Integer call() throws Exception {
        
        System.out.println("Hallo Welt.");
        
        CsvSettings settings = new CsvSettings();
        ValidationConfig config = IniFileReader.readFile(configFile);
        
        String firstLineIsHeader = config.getConfigValue(CsvConfig.SETTING_SECTION_PARAMETER, CsvConfig.SETTING_FIRSTLINE_IS_HEADER);
        settings.setValue(CsvConfig.SETTING_FIRSTLINE_IS_HEADER, Boolean.parseBoolean(firstLineIsHeader) ? CsvConfig.SETTING_FIRSTLINE_AS_HEADER : CsvConfig.SETTING_FIRSTLINE_AS_VALUE);
        
        String valueDelimiter = config.getConfigValue(CsvConfig.SETTING_SECTION_PARAMETER, CsvConfig.SETTING_VALUEDELIMITER);
        settings.setValue(CsvConfig.SETTING_VALUEDELIMITER, valueDelimiter.replace("\\", ""));

        String valueSeparator = config.getConfigValue(CsvConfig.SETTING_SECTION_PARAMETER, CsvConfig.SETTING_VALUESEPARATOR);
        settings.setValue(CsvConfig.SETTING_VALUESEPARATOR, valueSeparator.replace("\\", ""));

        String encoding = config.getConfigValue(CsvConfig.SETTING_SECTION_PARAMETER, CsvConfig.SETTING_ENCODING);
        settings.setValue(CsvReader.ENCODING, encoding);
        
        System.out.println(settings);

        
        boolean failed = false;
        Csv2Parquet csv2parquet = new Csv2Parquet();
        failed = csv2parquet.run(csvFile.toPath(), outputDir.toPath(), settings);

        return failed ? 1 : 0;
    }


    public static void main(String... args) {
        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }
}