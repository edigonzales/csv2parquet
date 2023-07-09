package ch.so.agi.csv2parquet;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import org.interlis2.validator.Validator;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iom_j.csv.CsvReader;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.inifile.IniFileReader;
import ch.interlis.iox_j.validator.ValidationConfig;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
        name = "csv2parquet",
        description = "Convert csv files to parquet files.",
        //version = "csv2parquet version 0.0.1",
        mixinStandardHelpOptions = true,
        
        headerHeading = "%n",
        //synopsisHeading = "%n",
        descriptionHeading = "%nDescription: ",
        parameterListHeading = "%nParameters:%n",
        optionListHeading = "%nOptions:%n"
      )
public class App implements Callable<Integer> {
    
    @Option(names = { "-c", "--config" }, required = false, description = "The configuration file.")
    String configFileName;

    @Option(names = { "--id" }, required = false, description = "The identifier of the dataset or sub dataset")
    String identifier;

    @Option(names = { "-i", "--input" }, required = true, description = "The input csv file.") 
    File csvFile;
    
    @Option(names = { "-o", "--output" }, required = false, description = "The output directory where the resulting parquet file is written to.") 
    File outputDir;
    
    @Option(names = { "--disableValidation" }, defaultValue = "false", required = false, description = "Disable CSV validation (if model ist set).") 
    Boolean disableValidation; 

    @Option(names = { "--meta" }, defaultValue = "false", required = false, description = "Create metadata file.") 
    Boolean meta; 
    
    @Option(names = { "--trace" }, defaultValue = "false", required = false, description = "Enable trace logging.") 
    Boolean trace;
    
    @Override
    public Integer call() throws Exception {
        
        EhiLogger.getInstance().setTraceFilter(!trace);
        
        // TODO
        // Fake it, till you make it.
        // Zukünftig soll ilidata aus einem INTERLIS-Repo geladen werden.
        File configFile = null;
        if (configFileName != null) {
            if (configFileName.startsWith("ilidata")) {
                configFile = Utils.copyResourceToTmpDir(configFileName.split(":")[1]);
                if (configFile != null) {
                    EhiLogger.traceState("config file <"+configFile.getAbsolutePath()+">.");                
                }            
            } else {
                configFile = new File(configFileName);
            }            
        }

        // Config-File parsen und die Prosanamen der Parameter auf die technischen Namen mappen.
        // Die technischen Namen sind nicht dazu gedacht von Benutzern in eine Datei zu tippen.
        // Nachtrag: Ist im Prinzip unnötig und ich könnte eine Abürzung nehmen, da die Parameter 
        // im CsvReader nicht direkt mit Settings gesteuert werden, sondern explizit mit Setter-
        // Methoden.
        Settings settings = new Settings();
        if (configFile != null) {
            try {
                // TODO Handling, falls nicht vorhanden.
                settings = SettingsMapper.run(configFile, identifier);            
            } catch (IOException e) {
                e.printStackTrace();
                return 1;
            }            
        }
        
        if (outputDir == null) {
            outputDir = csvFile.getParentFile();
        }
        
        if (!disableValidation && settings.getValue(Validator.SETTING_MODELNAMES)!=null) {   
            String[] csvFiles = { csvFile.getAbsolutePath() };
            boolean validationOk = new CsvValidator().validate(csvFiles, settings);
            if (!validationOk) {
                return 2;
            }
        }
        
        // TODO Exception handling im Vergleich zu anderen Klassen?!
        if (meta && settings.getValue(Validator.SETTING_MODELNAMES)!=null) {
            Toml2Xtf toml2Xtf = new Toml2Xtf();
            toml2Xtf.run(configFile, outputDir.toPath());
        }
        
        Csv2Parquet csv2parquet = new Csv2Parquet();
        boolean failed = csv2parquet.run(csvFile.toPath(), outputDir.toPath(), settings);

        return failed ? 3 : 0;
    }


    public static void main(String... args) {
        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }
}