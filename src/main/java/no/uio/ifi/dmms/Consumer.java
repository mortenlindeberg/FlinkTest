package no.uio.ifi.dmms;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class Consumer extends Thread {
    private int port;
    private String hostName;

    public Consumer() {
        this.port = 1080;
        this.hostName = "localhost";
    }

    public void run() {
        System.out.println("- Starting Flink - ");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream(hostName, port);

        DataStream<Tuple2<Long, Integer>> stream =
                text.flatMap(new LineSplitter())
                        //.keyBy(1)
                        .timeWindowAll(Time.seconds(5))
                        .max(1);

        stream.print();

        // execute program
        try {
            env.execute("Flink Sumarize event ID within 5 seconds time window");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<Long, Integer>> {

        public void flatMap(String value, Collector<Tuple2<Long, Integer>> out) {
            String[] attributes = value.split(",");
            out.collect(new Tuple2(Long.parseLong(attributes[0]), Integer.parseInt(attributes[1])));
        }
    }

}
