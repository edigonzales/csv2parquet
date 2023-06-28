package ch.so.agi.csv2parquet;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class Utils {

    public static File copyResourceToTmpDir(String resource) {
        try {
            InputStream is = Utils.class.getClassLoader().getResourceAsStream(resource);
            if (is==null) return null;
            Path exportDir = Files.createTempDirectory("csv2parquet");
            Path exportedFile = exportDir.resolve(new File(resource).getName());
            Files.copy(is, exportedFile, StandardCopyOption.REPLACE_EXISTING);
            return exportedFile.toFile();            
        } catch (IOException e) {
            return null;
        }
    }
}
