package output;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import utils.PropertieUtils;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 参数Text存放的是一行内容
 * 处理核心 输出逻辑
 */
public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    private Map<String, FSDataOutputStream> fsDataOutputStreamMap = new HashMap<String, FSDataOutputStream>();
//    private FSDataOutputStream fsDataOutputStream = null;
    private FileSystem fs = null;
    private String tableName = null;
    private Path outPutPath = null;
    private Configuration conf = null;

    // 通过构造方法初始化属性值
    public FilterRecordWriter(TaskAttemptContext job) {
        conf = job.getConfiguration();
        String parseLogTime = conf.get("parseLogTime");
        String preoutputPath = conf.get("preoutputPath");
        String path = conf.get("fs.defaultFS");
        try {
             fs = FileSystem.get(new URI(path), conf, "hdfs");
//            fs = FileSystem.get(conf);
        }catch (Exception e){
            e.printStackTrace();
        }
        Set<String> allTableNameList = PropertieUtils.getAllTableNameList();
        Iterator<String> tableIteror = allTableNameList.iterator();
        while(tableIteror.hasNext()){
            String table_Name = tableIteror.next();
            Path outPutPath = getTableDataPathByEvent(table_Name, parseLogTime, preoutputPath);
            try {
                FSDataOutputStream OutputStream = fs.create(outPutPath, false);
                fsDataOutputStreamMap.put(table_Name,OutputStream);
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    /**
     * 传入的参数就是一行数据
     * 写出数据到文件
     * @param key reduce输出的key
     * @param value reduce输出的value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 得到event和role
        String strKey = key.toString();
        String[] ts_messageStrAaary = strKey.split("\001");
        try {
            String ts = convertTime(ts_messageStrAaary[0]);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String message = ts_messageStrAaary[1];

        JSONObject jsObj = JSONObject.parseObject(message);

        String event = jsObj.getString("event");
        String role = jsObj.getString("role");
        // 获取输出流
        tableName = getTableNameByEvent(event,role);
        FSDataOutputStream outputStream = fsDataOutputStreamMap.get(tableName);

        // 输出数据
        String fieldsValueStr = getFieldsValueStr(strKey);;
        if(outputStream == null){
            System.out.println("====输出流为空======");
        }else{
            outputStream.write(fieldsValueStr.getBytes());
            outputStream.write("\r\n".getBytes());
        }
//        fsDataOutputStream.flush();
    }

    /**
     * 关闭输出流
     * @param context mr上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // 关闭资源
        for(String tableName : fsDataOutputStreamMap.keySet()){
            FSDataOutputStream stream = fsDataOutputStreamMap.get(tableName);
            if (stream != null) {
                stream.close();
            }
        }
    }

    /**
     * 将原始日志数据进行切分
     * @param strKey
     * @return
     */
    private String getFieldsValueStr(String strKey){
        String fieldsValueStr = "";
        String strNotNull = "";

        String[] ts_messageStrAaary = strKey.split("\001");
        String convertedTime = null;
        try {
            convertedTime = convertTime(ts_messageStrAaary[0]);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String message = ts_messageStrAaary[1].trim();

        // 得到hive表中的所有字段
        JSONObject jsObj = JSONObject.parseObject(message);

        String event = jsObj.getString("event");
        String role = jsObj.getString("role");
        tableName = getTableNameByEvent(event,role);
        String[] fieldArray = new String[0];
        // 配置文件中表的字段
        try {
            fieldArray = getFieldsByTableName(tableName);
        }catch (Exception e){
            e.printStackTrace();
            System.out.println(tableName+"该表名不存在对应字段");
        }


        // 根据表的字段得到值

        if(fieldArray.length == 1){
            return fieldsValueStr;
        }
        for(int i=0; i < fieldArray.length; i++){
            String field = fieldArray[i];
            if(field.equals("ts")){
                fieldsValueStr = fieldsValueStr + convertedTime+"\001";
            }else{
                if(i != fieldArray.length-1){
                    if(jsObj.get(field) == null){
                        fieldsValueStr = fieldsValueStr + strNotNull+"\001";
                    }else{
                        fieldsValueStr = fieldsValueStr + jsObj.get(field).toString()+"\001";
                    }
                }else {
                    if(jsObj.get(field) == null){
                        fieldsValueStr = fieldsValueStr + strNotNull;
                    }else {
                        fieldsValueStr = fieldsValueStr + jsObj.get(field).toString();
                    }
                }
            }
        }
        return fieldsValueStr;
    }
    /**
     * 根据event得到表名
     * @param event
     * @return 得到hive表名
     */
    public String getTableNameByEvent(String event, String role){
        String tableName = "";
        Properties evemt_hivetablePrpo = PropertieUtils.getProp("evemt_hivetablePrpo");
        if(event.equals("LOGIN")){
            if(role.equals("STUDENT")) return evemt_hivetablePrpo.getProperty("STUDENT_LOGIN");
            if(role.equals("TEACHER")) return evemt_hivetablePrpo.getProperty("TEACHER_LOGIN");
        }
            if (event.equals("KEEPALIVE")){
            if(role.equals("STUDENT")) return evemt_hivetablePrpo.getProperty("STUDENT_KEEPALIVE");
            if(role.equals("TEACHER")) return evemt_hivetablePrpo.getProperty("TEACHER_KEEPALIVE");
        }
        tableName = evemt_hivetablePrpo.getProperty(event);
        return tableName;
    }

    /**
     * 根据表名得到表中的所有字段
     */
    public String[] getFieldsByTableName(String tableName){
        Properties hiveTable_tablefieldsProp = PropertieUtils.getProp("hiveTable_tablefieldsProp");
        String fields = hiveTable_tablefieldsProp.getProperty(tableName);

        return fields.trim().split(",");
    }

    /**
     * 根据表名得到表数据的保存路径
     * @param tableName
     * @return
     */
    public Path getTableDataPathByEvent(String tableName, String parselogTime, String preoutputPath){

//        Path path = new Path("hdfs://hadoop1:8020/tmp/result/"+tableName+"/"+tableName);
        Path path = new Path(preoutputPath + "/"+ tableName+"/"+parselogTime+"/"+tableName);
        return path;
    }

    /**
     * 时间格式转换
     * @param time 原始日志时间
     * @return
     * @throws ParseException
     */
    public String convertTime(String time) throws ParseException{
        // 处理时间 由 yyyy-MM-dd'T'HH:mm:ss.SSS Z --> yyyy-MM-dd HH:mm:ss
        String date = time.replace("Z", " UTC");//注意是空格+UTC
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS Z");//注意格式化的表达式
            Date date1 = format.parse(date);
            SimpleDateFormat aDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String format1 = aDate.format(date1);
            return format1;
    }
}

