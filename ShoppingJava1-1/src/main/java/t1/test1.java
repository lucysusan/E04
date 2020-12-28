package t1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class test1 {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                //拆分行,下标从0开始
                String[] temp=word.toString().split(",");
                //首先筛选出time_stamp=1111的商品   [5]
                //对action_type，如果是1，2，3（不等于0）则计数  [6]
                if(temp[5].equals("1111"))
                    if(!temp[6].equals("0"))
                    {
                        //以商品id   item_id作为它的key     [1]
                        context.write(new Text(temp[1]), one);
                    }


            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    //新job的reduce
    public static class AutoReducer extends Reducer<IntWritable,Text,Text, NullWritable>{
        private int count =0;
        public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
            for(Text v:values)
            {
                if(count==100)
                    return;
                count++;
                String v_string=v.toString();
                String res=count+"- item_id: "+v_string+", times: "+key.toString();
                context.write(new Text(res),NullWritable.get());
            }
        }
    }


    //自定义降序排列
    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    //driver
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf,args);  //2.0

        String[] remainingArgs = optionParser.getRemainingArgs();          //2.0
        if((remainingArgs.length!=2) && (remainingArgs.length != 4)){                   //2.0
            System.err.println("Usage:wordcount <in> <out> [-skip skipPatternFile]");      //2.0
            System.exit(2);    //2.0
        }        //2.0
        @SuppressWarnings("deprecation")             //1.0

        // 定义一个临时目录
        Path tempDir = new Path("code1-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));  //new

        Job job = Job.getInstance(conf,"code1");
        job.setJarByClass(test1.class);         //
        job.setMapperClass(TokenizerMapper.class);            //Mapper
        job.setCombinerClass(IntSumReducer.class);            //Combine  词频降序操作
        job.setReducerClass(IntSumReducer.class);             //Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> otherArgs = new ArrayList<String>();
        for(int i=0;i< remainingArgs.length;++i){

            otherArgs.add(remainingArgs[i]);

        }

        FileInputFormat.addInputPath(job,new Path(otherArgs.get(0)));    //词频降序操作
        FileOutputFormat.setOutputPath(job,tempDir);     //词频降序操作

        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);

        if (job.waitForCompletion(true))
        // 只有当job任务成功执行完成以后才开始sortJob，参数true表明打印verbose信息
        {
            Job sortJob = Job.getInstance(conf, "sort");

            sortJob.setJarByClass(test1.class);
            FileInputFormat.addInputPath(sortJob, tempDir);
            sortJob.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
            sortJob.setMapperClass(org.apache.hadoop.mapreduce.lib.map.InverseMapper.class);
            sortJob.setReducerClass(AutoReducer.class);             //Reducer   自己添加的
            // InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换

            sortJob.setNumReduceTasks(1);
            // 将Reducer的个数限定为1，最终输出的结果文件就是一个

            FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs.get(1)));
            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);
            sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
            /*
             * Hadoop默认对IntWritable按升序排序，而我们需要的是按降序排列。
             * 因此我们实现了一个IntWritableDecreasingCompatator类，并指定使用这个自定义的Comparator类，
             * 对输出结果中的key（词频）进行排序
             */
            System.exit(sortJob.waitForCompletion(true) ? 0 : 1);

        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
