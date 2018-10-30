package reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 *  处理日志的reduce
 */
public class LogReducer extends Reducer<Text, Text, Text, NullWritable>{

    /**
     *
     * @param key event_role
     * @param values 集合中的元数内容：ts_message
     * @param context mr上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        //遍历集合，输出数据

        for (Iterator iter = values.iterator(); iter.hasNext();) {
            String k = iter.next().toString();
            k = k + "\r\n";
//            System.out.print("======"+"reduce"+"======="+"\n");
//            System.out.print(k+"\n");
            context.write(new Text(k), NullWritable.get());
        }
    }
}
