package ch01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 描述
 *
 * @author lcz
 * @version 1.0
 * @date 2022/11/28 18:13:06
 */
public class StreamingJob {
    public static void main(String[] args) throws Exception {
        //参数处理 参数名 参数值
        ParameterTool params = ParameterTool.fromArgs(args);

        //获取流计算执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String ip = "127.0.0.1";
        int port = 7777;

        if(params.has("ip")){
            ip = params.get("ip");
        }

        if(params.has("port")){
            port = Integer.parseInt(params.get("port"));
        }
        DataStreamSource<String> ds = env.socketTextStream(ip, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> anly = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split("\\s");
                for (int i = 0; i < split.length; i++) {
                    out.collect(new Tuple2<>(split[i], 1));
                }
            }
        })
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tp -> tp.f0)
                .sum(1);
        anly.print();

        env.execute("Java Flink Demo!");
    }
}
