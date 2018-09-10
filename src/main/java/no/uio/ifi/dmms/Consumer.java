package no.uio.ifi.dmms;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

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

        DataStream<Tuple2<Long, Double>> stream =
                text.flatMap(new LineSplitter())
                        .timeWindowAll(Time.seconds(5))
                        .max(1);

        stream.print();


        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

        ElasticsearchSinkFunction<Tuple2<Long, Double>> sinkFunction = new ElasticsearchSinkFunction<Tuple2<Long, Double>>() {
            @Override
            public void process(Tuple2<Long, Double> tuple, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                Map<String,String> json = new HashMap<>();
                json.put("time",""+tuple.f0);
                json.put("power",""+tuple.f1);
                requestIndexer.add(Requests.indexRequest().index("gpx").type("float").source(json));
            }
        };
        ElasticsearchSink.Builder<Tuple2<Long, Double>> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts, sinkFunction);
        stream.addSink(esSinkBuilder.build());


        // execute program
        try {
            env.execute("Flink Sumarize event ID within 5 seconds time window");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<Long, Double>> {

        public void flatMap(String value, Collector<Tuple2<Long, Double>> out) {
            String[] attributes = value.split(",");
            out.collect(new Tuple2(Long.parseLong(attributes[0]), Double.parseDouble(attributes[3])));
        }
    }
}
