package no.uio.ifi.dmms;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.util.*;


public class GpxHeartPowerPattern extends Thread {
    private static final Integer HR_THRESHOLD = 150;
    private int port;
    private String hostName;

    public GpxHeartPowerPattern() {
        this.port = 1080;
        this.hostName = "localhost";
    }

    public void run() {
        System.out.println("- Starting Flink - ");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream(hostName, port);

        DataStream<GpxEvent> rawStream = text.flatMap(new LineSplitter());
        DataStream<GpxEventAccumulator> gpxStream = rawStream
                        .timeWindowAll(Time.milliseconds(100))
                        .aggregate(new AveragePower());

        // Define warning pattern
        Pattern<GpxEvent, ?> warningPattern = Pattern.<GpxEvent>begin("start")
                .where(new SimpleCondition<GpxEvent>() {
                    @Override
                    public boolean filter(GpxEvent value) {
                        return value.getHr() <= HR_THRESHOLD;
                    }
                });

        PatternStream<GpxEvent> warningStream = CEP.pattern(rawStream.keyBy("timestamp"), warningPattern);

        DataStream<GpxAlert> warningResult = warningStream.select(new PatternSelectFunction<GpxEvent, GpxAlert>() {
            @Override
            public GpxAlert select(Map<String, List<GpxEvent>> pattern) throws Exception {
                GpxAlert out = new GpxAlert(pattern.get("start").get(0));
                return out;
            }
        });

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

        ElasticsearchSink.Builder<GpxEventAccumulator> sinkBuilder1 = new ElasticsearchSink.Builder<GpxEventAccumulator>(httpHosts, new PowerHeartInserter());
        sinkBuilder1.setBulkFlushMaxActions(1);
        gpxStream.addSink(sinkBuilder1.build());

        ElasticsearchSink.Builder<GpxAlert> sinkBuilder2 = new ElasticsearchSink.Builder<GpxAlert>(httpHosts, new HrWarningInserter());
        sinkBuilder2.setBulkFlushMaxActions(1);
        warningResult.addSink(sinkBuilder2.build());


        // execute program
        try {
            env.execute("Flink processing gpx from Bærumsrunden");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static final class LineSplitter implements FlatMapFunction<String, GpxEvent> {
        @Override
        public void flatMap(String value, Collector<GpxEvent> out) {
            String[] attributes = value.split(",");
            out.collect(new GpxEvent(
                    Long.parseLong(attributes[0]),
                    Double.parseDouble(attributes[3]),
                    Integer.parseInt(attributes[5]),
                    Float.parseFloat(attributes[1]),
                    Float.parseFloat(attributes[2])));
        }
    }

    public static class PowerHeartInserter implements ElasticsearchSinkFunction<GpxEventAccumulator> {
        @Override
        public void process(GpxEventAccumulator tuple, RuntimeContext ctx, RequestIndexer indexer) {
            Map<String, String> json = new HashMap<>();
            json.put("time", tuple.getTimestamp().toString());
            json.put("power", tuple.getPower().toString());
            json.put("hr", tuple.getHr().toString());
            json.put("location", tuple.getLat()+","+tuple.getLon());

            IndexRequest rqst = Requests.indexRequest()
                    .index("gpx")
                    .type("power")
                    .source(json);
            indexer.add(rqst);
        }
    }

    public static class HrWarningInserter implements ElasticsearchSinkFunction<GpxAlert> {
        @Override
        public void process(GpxAlert tuple, RuntimeContext ctx, RequestIndexer indexer) {
            Map<String, String> json = new HashMap<>();
            json.put("time", tuple.getTimestamp().toString());
            json.put("factor", tuple.getFactor().toString());
            json.put("location", tuple.getLat()+","+tuple.getLon());

            IndexRequest rqst = Requests.indexRequest()
                    .index("warn")
                    .type("hr")
                    .source(json);
            indexer.add(rqst);
        }
    }

    private static final class AveragePower implements AggregateFunction<GpxEvent,GpxEventAccumulator,GpxEventAccumulator> {
        /* The intention is that this accumulates the average power and heartrate, but for timestamp, lat and lon - we use the values form the newest tuple */
        @Override
        public GpxEventAccumulator createAccumulator() {
            return new GpxEventAccumulator();
        }

        @Override
        public GpxEventAccumulator add(GpxEvent value, GpxEventAccumulator ac) {
            Long timestamp;
            Float lat;
            Float lon;

            if (value.getTimestamp() >= ac.getTimestamp()) {
                timestamp = value.getTimestamp();
                lat = value.getLat();
                lon = value.getLon();
            }
            else {
                timestamp = ac.getTimestamp();
                lat = ac.getLat();
                lon = ac.getLon();
            }

            return new GpxEventAccumulator(timestamp, value.getPower() + ac.getPower(), value.getHr() + ac.getHr(), lat, lon,ac.getCount() + 1L);
        }

        @Override
        public GpxEventAccumulator merge(GpxEventAccumulator a, GpxEventAccumulator b) {
            Long timestamp;
            Float lat;
            Float lon;
            if (a.getTimestamp() >= b.getTimestamp()) {
                timestamp = a.getTimestamp();
                lat = a.getLat();
                lon = a.getLon();
            }
            else {
                timestamp = b.getTimestamp();
                lat = b.getLat();
                lon = b.getLon();
            }
            return new GpxEventAccumulator(timestamp, a.getPower() + b.getPower(), a.getHr() + b.getHr(),lat, lon, a.getCount() + b.getCount());
        }

        @Override
        public GpxEventAccumulator getResult(GpxEventAccumulator ac) {
            return new GpxEventAccumulator(ac.getTimestamp(),(Double)(ac.getPower() / ac.getCount()), (ac.getHr() / ac.getCount().intValue()), ac.getLat(), ac.getLon(), ac.getCount());
        }
    }
}