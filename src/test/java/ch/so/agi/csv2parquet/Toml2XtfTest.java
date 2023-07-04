package ch.so.agi.csv2parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.interlis2.validator.Validator;
import org.junit.jupiter.api.Test;

import ch.ehi.basics.settings.Settings;

public class Toml2XtfTest {
    
    @Test
    public void single_resource_Ok() throws Exception {
        // Prepare
//        Path configPath = Paths.get("src/test/data/amtliche_vermessung_statistik/ch.so.agi.amtliche_vermessung_statistik.toml");
//        Path configPath = Paths.get("src/test/data/abfallmengen_gemeinden/ch.so.afu.abfallmengen_gemeinden.toml");
//        Path configPath = Paths.get("src/test/data/bewilligte_erdwaermeanlagen/ch.so.afu.bewilligte_erdwaermeanlagen.toml");
//        Path configPath = Paths.get("src/test/data/steuerfuesse/ch.so.agem.steuerfuesse.toml");
        Path configPath = Paths.get("src/test/data/kantonale_gebaeude/ch.so.hba.kantonale_gebaeude.toml");
        Path outputPath = Paths.get("build/test/data/toml2xtf/");
        outputPath.toFile().mkdirs();

        // Run
        Toml2Xtf toml2Xtf = new Toml2Xtf();
        toml2Xtf.run(configPath.toFile(), outputPath);

        // Validate
        Settings settings = new Settings();
        settings.setValue(Validator.SETTING_ILIDIRS, Validator.SETTING_DEFAULT_ILIDIRS + ";src/main/resources/ili/");

        Path xtfFile = Paths.get(outputPath.toFile().getAbsolutePath(), "meta-ch.so.hba.kantonale_gebaeude.xtf");
        boolean valid = Validator.runValidation(xtfFile.toString(), settings);
        assertTrue(valid);
        
        String content = new String(Files.readAllBytes(xtfFile));
        assertTrue(content.contains("<Description>Anzahl kantonale (nicht-öffentliche) Ladestationen</Description>"));
    }

}
