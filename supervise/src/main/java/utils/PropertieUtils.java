package utils;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

/**
 * 配置文件工具类
 */
public class PropertieUtils {

    private static Properties evemt_hivetablePrpo = new Properties();
    private static Properties hiveTable_tablefieldsProp = new Properties();

    static {
        try {
            InputStream evemt_hivetableIn = PropertieUtils.class
                    .getClassLoader().getResourceAsStream("event_to_table.properties");
            InputStream hiveTable_tablefieldsIn = PropertieUtils.class
                    .getClassLoader().getResourceAsStream("table_to_column.properties");

            // 获取到配置文件内容
            evemt_hivetablePrpo.load(evemt_hivetableIn);
            hiveTable_tablefieldsProp.load(hiveTable_tablefieldsIn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据propName得到相应的Prop对象
     * @param propName 对象名称
     * @return Prop对象
     */
    public static Properties getProp(String propName){
        if(propName.equals("evemt_hivetablePrpo")){
            return evemt_hivetablePrpo;
        }
        if(propName.equals("hiveTable_tablefieldsProp")){
            return hiveTable_tablefieldsProp;
        }
        return null;
    }
    /**
     * 获取指定key对应的value
     * @param propName prop对象名称
     * @param key 配置文件的属性名称
     * @return value 配置文件中的属性值
     */
    public static String getProperty(String propName, String key) {
        return getProp(propName).getProperty(key);
    }

    /**
     * 获取到所有表名
     * @return
     */
    public static Set<String> getAllTableNameList(){
        HashMap<String, String> map = new HashMap<>();
        Set<String> tableNameSet = hiveTable_tablefieldsProp.stringPropertyNames();
        return tableNameSet;
    }
}

