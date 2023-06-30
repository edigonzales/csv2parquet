package ch.so.agi.csv2parquet;

import java.io.File;
import java.nio.file.Path;

import org.tomlj.Toml;
import org.tomlj.TomlParseResult;

import ch.ehi.basics.settings.Settings;
import ch.so.agi.csv2parquet.meta.ModelDescription;

public class Meta2Xtf {

    // TODO: 
    // - 

    // Wenn wir eine Config-Datei haben, wird sie bereits geparsed. Man muss einfach
    // noch mehr auslesen und in die Settings schreiben. Und ein paar Konstanten mehr.
    // Meta2Xtf bedingt zwingend ein Modell (nicht bloss eine Config-Datei) -> if().
    // Ah Mist, das Modell muss ich noch kompilieren -> LOCAL_REPO-Konstante. Oder im SettingsMapper kompilieren.
    public boolean run(Settings settings, Path outputPath) {

        //ModelDescription.getDescriptions(null, false, null, null);
        
        
        return false;
    }
}
