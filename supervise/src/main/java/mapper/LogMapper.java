package mapper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.StringUtils;

import java.io.IOException;

/**
 * 处理原始日志的mapper
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, Text>{
    StringUtils strUtils = new StringUtils();
//    int i = 0;
    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        Text mapKey = new Text();
        Text mapValue = new Text();

        // 得到一行原始日志数据
        String line = value.toString();
        String subLine = strUtils.getSubString("{",line);
        JSONObject jsonLine = null;
        try {
            jsonLine = JSON.parseObject(subLine);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("==========有问题的日志数据============");
            System.out.println(subLine);
            return;
        }

        String ts = jsonLine.getString("@timestamp");
        String message = jsonLine.getString("message");

        // 获取map输出key
        JSONObject jsonMessage = JSONObject.parseObject(message);
        String event = jsonMessage.getString("event");
        String role = jsonMessage.getString("role");
        String mapOutputKey = event+"\001"+role;
        mapKey.set(mapOutputKey);
        // 获取map输出value
        String mapOutputValue = ts+"\001"+message;

        JSONObject jsObj = null;

        mapValue.set(mapOutputValue);

        //输出该条日志
        context.write(mapKey,mapValue);
    }
}
