package no.uio.ifi.dmms;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
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

        DataStream<Tuple5<Long, Double, Float, Float, Long>> stream =
                text.flatMap(new LineSplitter())
                        .timeWindowAll(Time.seconds(1))
                        .aggregate(new AveragePower());

        stream.print();


        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
        ElasticsearchSink.Builder<Tuple5<Long, Double, Float, Float, Long>> sinkBuilder =
                new ElasticsearchSink.Builder<Tuple5<Long, Double, Float, Float, Long>>(httpHosts, new PowerInserter());
        sinkBuilder.setBulkFlushMaxActions(1);
        stream.addSink(sinkBuilder.build());

        // execute program
        try {
            env.execute("Flink finds max value each second");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final class AveragePower
            implements AggregateFunction<Tuple4<Long, Double, Float, Float>,Tuple5<Long, Double, Float, Float, Long>, Tuple5<Long, Double, Float, Float, Long>> {

        private Long count;

        public AveragePower() {
            this.count = 0L;
        }
        @Override
        public Tuple5<Long, Double, Float, Float, Long> createAccumulator() {
            return new Tuple5<>(0L,0D,0F,0F,0L);
        }

        @Override
        public Tuple5<Long, Double, Float, Float, Long> add(Tuple4<Long, Double, Float, Float> value, Tuple5<Long, Double, Float, Float, Long> ac) {
            return new Tuple5<>(value.f0 + ac.f0, value.f1 + ac.f1, value.f2 + ac.f2, value.f3 + ac.f3, ac.f4 + 1L);
        }

        @Override
        public Tuple5<Long, Double, Float, Float, Long> merge(Tuple5<Long, Double, Float, Float, Long> a, Tuple5<Long, Double, Float, Float, Long> b) {
            return new Tuple5<>(a.f0 + b.f0, a.f1 + b.f1, a.f2 + b.f2, a.f3 + b.f3, a.f4 + b.f4);
        }

        @Override
        public Tuple5<Long, Double, Float, Float, Long> getResult(Tuple5<Long, Double, Float, Float, Long> ac) {
            return new Tuple5<>((Long)(ac.f0 / ac.f4),(Double)(ac.f1 / ac.f4), (Float)(ac.f2 / ac.f4), (Float)(ac.f3 / ac.f4), (Long)ac.f4);
        }
    }

    private static final class LineSplitter implements FlatMapFunction<String, Tuple4<Long, Double, Float, Float>> {
        @Override
        public void flatMap(String value, Collector<Tuple4<Long, Double, Float, Float>> out) {
            String[] attributes = value.split(",");
            out.collect(new Tuple4(
                    Long.parseLong(attributes[0]),
                    Double.parseDouble(attributes[3]),
                    Float.parseFloat(attributes[1]),
                    Float.parseFloat(attributes[2])));
        }
    }

    public static class PowerInserter implements ElasticsearchSinkFunction<Tuple5<Long, Double, Float, Float, Long>> {
        @Override
        public void process(Tuple5<Long, Double, Float, Float, Long> tuple, RuntimeContext ctx, RequestIndexer indexer) {
            Map<String, String> json = new HashMap<>();
            json.put("time", tuple.f0.toString());
            json.put("power", tuple.f1.toString());
            json.put("location", tuple.f2+","+tuple.f3);
            json.put("count", tuple.f4.toString());

            IndexRequest rqst = Requests.indexRequest()
                    .index("gpx")
                    .type("power")
                    .source(json);
            indexer.add(rqst);
        }
    }
}