package cs523.hvk;

import java.io.IOException;
import java.util.Properties;

public final class AppProperties {

    private static Properties appProperties = new Properties();

    private AppProperties(){
    }

    static {
        try {
            appProperties.load(AppProperties.class.getResourceAsStream("/application.properties"));
        }catch(IOException ex){
            System.err.println("Error loading application.properties");
            ex.printStackTrace();
        }
    }

    public static String get(String key){
        return appProperties.getProperty(key);
    }
    
}
