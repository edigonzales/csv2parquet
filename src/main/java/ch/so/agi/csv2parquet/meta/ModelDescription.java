package ch.so.agi.csv2parquet.meta;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.tomlj.TomlParseResult;
import org.tomlj.TomlTable;

import ch.interlis.ili2c.Ili2c;
import ch.interlis.ili2c.Ili2cException;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.metamodel.AttributeDef;
import ch.interlis.ili2c.metamodel.CompositionType;
import ch.interlis.ili2c.metamodel.CoordType;
import ch.interlis.ili2c.metamodel.Domain;
import ch.interlis.ili2c.metamodel.Element;
import ch.interlis.ili2c.metamodel.EnumerationType;
import ch.interlis.ili2c.metamodel.FormattedType;
import ch.interlis.ili2c.metamodel.Model;
import ch.interlis.ili2c.metamodel.NumericType;
import ch.interlis.ili2c.metamodel.PolylineType;
import ch.interlis.ili2c.metamodel.SurfaceOrAreaType;
import ch.interlis.ili2c.metamodel.Table;
import ch.interlis.ili2c.metamodel.TextType;
import ch.interlis.ili2c.metamodel.Topic;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ili2c.metamodel.Type;
import ch.interlis.ilirepository.IliManager;

public class ModelDescription {
    private static final String ILI_DIR_NAME = "ili";

    private static final List<String> META_TOML_CONFIG_SECTIONS = new ArrayList<String>() {{
        add("meta");
        add("config");
    }};

    public static Map<String, ClassDescription> getDescriptions(String modelName, boolean override, TomlParseResult metaTomlResult, File iliDir) throws IOException, Ili2cException {
        TransferDescription td = getTransferDescriptionFromModelName(modelName, iliDir.getAbsolutePath());
        
        Map<String, ClassDescription> classDescriptions = new HashMap<>();

        for (Model model : td.getModelsFromLastFile()) {
            if (!model.getName().equalsIgnoreCase(modelName)) {
                continue;
            }

            Iterator<Element> modeli = model.iterator();
            while (modeli.hasNext()) {
                Object tObj = modeli.next();

                if (tObj instanceof Domain) {
                    // Falls ich die Werte will.
                } else if (tObj instanceof Table) {
                    Table tableObj = (Table) tObj;
                    // https://github.com/claeis/ili2c/blob/ccb1331428/ili2c-core/src/main/java/ch/interlis/ili2c/metamodel/Table.java#L30
                    if (tableObj.isIdentifiable()) {
                        // Abstrakte Klasse:
                        // Kann ich ignorieren, da alles was ich wissen will (? Attribute und
                        // Beschreibung) in den spezialisieren Klassen vorhanden ist.
                    } else {
                        // Struktur:
                        // Momentan nicht von Interesse.
                    }
                } else if (tObj instanceof Topic) {
                    Topic topic = (Topic) tObj;
                    Iterator<?> iter = topic.getViewables().iterator();

                    while (iter.hasNext()) {
                        Object obj = iter.next();

                        // Viewable waere "alles". Was ist sinnvoll/notwendig fuer unseren Usecase?
                        // Domains?
                        // Momentan nur Table beruecksichtigen.
                        if (obj instanceof Table) {
                            Table table = (Table) obj;

                            // Abstrakte Klasse oder Struktur:
                            // Abstrakte Klasse interessiert uns nicht, da alle
                            // Attribute in der spezialisierte Klass vorhanden sind.
                            // Struktur interessiert uns vielleicht spaeter aber
                            // zum jetzigen Zeitpunkt brauchen wir dieses Wissen
                            // nicht.
                            if (table.isAbstract() || !table.isIdentifiable()) {
                                continue;
                            }

                            ClassDescription classType = new ClassDescription();
                            classType.setName(table.getName());
                            classType.setTitle(table.getMetaValue("title"));
                            classType.setDescription(table.getDocumentation());
                            classType.setModelName(modelName);
                            classType.setTopicName(topic.getName());

                            Iterator<?> attri = table.getAttributes();

                            List<AttributeDescription> attributes = new ArrayList<>();
                            while (attri.hasNext()) {
                                Object aObj = attri.next();
                                AttributeDescription attributeType = new AttributeDescription();

                                if (aObj instanceof AttributeDef) {
                                    AttributeDef attr = (AttributeDef) aObj;
                                    attributeType.setName(attr.getName());
                                    attributeType.setDescription(attr.getDocumentation());

                                    Type type = attr.getDomainResolvingAll();
                                    attributeType.setMandatory(type.isMandatory() ? true : false);

                                    if (type instanceof TextType) {
                                        TextType t = (TextType) type; 
                                        attributeType.setDataType(t.isNormalized() ? DataType.TEXT : DataType.MTEXT);
                                    } else if (type instanceof NumericType) {
                                        NumericType n = (NumericType) type;
                                        attributeType.setDataType(n.getMinimum().getAccuracy() == 0 ? DataType.INTEGER : DataType.DOUBLE);
                                    } else if (type instanceof EnumerationType) {
                                        EnumerationType e = (EnumerationType) type;
                                        // Wenn man selber BOOLEAN definiert muss man hier nachziehen. Dann muessen
                                        // wohl die Werte ausgelesen werden e.getEnumeration() 
                                        if (attr.isDomainBoolean()) {
                                            attributeType.setDataType(DataType.BOOLEAN);
                                        } else {
                                            attributeType.setDataType(DataType.ENUMERATION);
                                        }
                                    } else if (type instanceof FormattedType) {
                                        FormattedType f = (FormattedType) type;
                                        String format = f.getFormat();
                                        if (format.contains("Year") && !format.contains("Hours")) {
                                            attributeType.setDataType(DataType.DATE);
                                        } else if (format.contains("Year") && format.contains("Hours")) {
                                            attributeType.setDataType(DataType.DATETIME);
                                        }
                                        // else if...
                                    } 
                                }
                                attributes.add(attributeType);
                            }
                            classType.setAttributes(attributes);
                            classDescriptions.put(classType.getQualifiedName(), classType);
                        } // else if... DOMAIN, etc.? DOMAIN nur, falls ich die Werte wirklich ausweisen will.
                    }
                }
            }
        }
        
        //if (override) overrideModelDescription(classDescriptions, metaTomlResult);

        return classDescriptions;
    }

    /*
    private static void overrideModelDescription(Map<String, ClassDescription> classDescriptions, TomlParseResult metaTomlResult) {
        Map<String, Object> metaTomlMap = metaTomlResult.toMap();
        for (Map.Entry<String, Object> entry : metaTomlMap.entrySet()) {
            if (!META_TOML_CONFIG_SECTIONS.contains(entry.getKey())) {
                String modelName = (String) entry.getKey();
                parseTopicDesc(classDescriptions, modelName, (TomlTable) entry.getValue());
            }      
        }
    }
    */
    
    private static void parseTopicDesc(Map<String, ClassDescription> classDescriptions, String modelName, TomlTable topicDescs) {
        for (Map.Entry<String, Object> entry : topicDescs.entrySet()) {
            String topicName = (String) entry.getKey();
            parseClassDesc(classDescriptions, modelName, topicName, (TomlTable) entry.getValue());   
        }
    }
    
    private static void parseClassDesc(Map<String, ClassDescription> classDescriptions, String modelName, String topicName, TomlTable classDescs) {
        for (Map.Entry<String, Object> entry : classDescs.entrySet()) {
            String className = (String) entry.getKey();
            
            TomlTable classDesc = (TomlTable) entry.getValue();
            String title = classDesc.getString("title");
            String description = classDesc.getString("description");

            String qualifiedClassName = modelName + "." + topicName + "." + className;
//            System.out.println("qualifiedClassName: " + qualifiedClassName);
//            System.out.println("className: " + className);
//            System.out.println("title: " + title);
//            System.out.println("description: " + description);
            
            ClassDescription classDescription = classDescriptions.get(qualifiedClassName);
//            System.out.println(classDescription);

            if (classDescription != null) {
                if (title != null) {
                    classDescription.setTitle(title);
                } 

                if (description != null) {
                    classDescription.setDescription(description);
                }
            }               
        }
    }

    private static TransferDescription getTransferDescriptionFromModelName(String modelName, String localRepo) throws  IOException, Ili2cException {
        IliManager manager = new IliManager();
        File ilicacheFolder = Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), ".ilicache_").toFile();        
        manager.setCache(ilicacheFolder);
        String repositories[] = new String[] { localRepo, "https://geo.so.ch/models", "http://models.interlis.ch/" };
        manager.setRepositories(repositories);
        ArrayList<String> modelNames = new ArrayList<String>();
        modelNames.add(modelName);
        Configuration config;
        config = manager.getConfig(modelNames, 2.3);
        TransferDescription td = Ili2c.runCompiler(config);

        if (td == null) {
            throw new IllegalArgumentException("INTERLIS compiler failed"); 
        }
        
        return td;
    }
}