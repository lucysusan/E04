package t1;

import java.io.IOException;
import java.util.StringTokenizer;

import java.util.*; //2.0
import java.io.BufferedReader; //2.0
import java.io.FileReader; //2.0

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

import org.apache.hadoop.mapreduce.Counter; //2.0
import org.apache.hadoop.util.StringUtils;  //2.0

public class test2 {
    public static class TokenizerMapper extends Mapper <Object, Text, Text, IntWritable>{

        private final static IntWritable one  = new IntWritable(1);
        private Text word = new Text();

        //2.0
        private Configuration conf;   //定义conf

        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration( );

            Path patternsPath=new Path("hdfs://localhost:9000/e04/data/user_info_skip.csv");
            String patternsFileName = patternsPath.getName( ).toString();
            parseSkipFile(patternsFileName);
        }

        //2.0
        private BufferedReader fis;     //定义fis
        private Set<String> patternsToSkip = new HashSet<String>();  //定义patternsToSkip

        private void parseSkipFile(String fileName){
            try{
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while((pattern = fis.readLine())!=null){
                    patternsToSkip.add(pattern);
                }
            }catch(IOException ioe){
                System.err.println("Caught exception while parsing the cached file " + StringUtils.stringifyException(ioe));
            }
        }


        //map
        //@Override    //2.0
        static enum CountersEnum { INPUT_WORDS }    //定义CountersEnum
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

            StringTokenizer itr = new StringTokenizer(value.toString());         //用StringTokenizer将字符串拆成单词
              while(itr.hasMoreTokens()){
                word.set(itr.nextToken());
                //把每一行拆解成一个一个属性,下标从0开始
                String[] temp=word.toString().split(",");

                //首先筛选出time_stamp=1111的商品   [5]
                //对action_type，如果是1，2，3（不等于0）则计数  [6]
                //购物者是年轻人   [0]
                if(temp[5].equals("1111"))
                    if(!temp[6].equals("0"))
                        if(! patternsToSkip.contains(temp[0])){
                            //以商家id   merchant_id作为它的key     [3]
                            context.write(new Text(temp[3]),one);       //用context.write收集<key,value>对
                            Counter counter = context.getCounter(CountersEnum.class.getName(),CountersEnum.INPUT_WORDS.toString());  //2.0
                            counter.increment(1);   //2.0

                        }
            }
        }
    }

    public static class IntSumReducer extends Reducer <Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;

            for(IntWritable val:values){                    //遍历迭代器values 得到同一key的所有value
                sum+=val.get();
            }
            result.set(sum);
            context.write(key,result);

        }
    }

    //新的job的reduce
    public static class AutoReducer extends Reducer<IntWritable,Text,Text, NullWritable>{
        private int count =0;
        public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
            for(Text v:values)
            {
                if(count==100)
                    return;
                count++;
                String v_string=v.toString();
                String res="("+v_string+","+key.toString()+")";
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
        Path tempDir = new Path("wordcount-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));  //new

        Job job = Job.getInstance(conf,"word count");
        job.setJarByClass(test2.class);         //
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

            sortJob.setJarByClass(test2.class);
            FileInputFormat.addInputPath(sortJob, tempDir);
            sortJob.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
            sortJob.setMapperClass(org.apache.hadoop.mapreduce.lib.map.InverseMapper.class);
            sortJob.setReducerClass(AutoReducer.class);             //Reducer   自己添加的

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
