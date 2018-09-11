package no.uio.ifi.dmms;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.elasticsearch.common.collect.Tuple;

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

        DataStream<Tuple4<Long, Double, Float, Float>> stream =
                text.flatMap(new LineSplitter())
                        .timeWindowAll(Time.seconds(1))
                        .max(1);

        stream.print();


        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
        ElasticsearchSink.Builder<Tuple4<Long, Double, Float, Float>> sinkBuilder =
                new ElasticsearchSink.Builder<Tuple4<Long, Double, Float, Float>>(httpHosts,new PowerInserter());
        sinkBuilder.setBulkFlushMaxActions(1);
        stream.addSink(sinkBuilder.build());

        // execute program
        try {
            env.execute("Flink finds max value each second");
        } catch (Exception e) {
            e.printStackTrace();
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

    public static class PowerInserter implements ElasticsearchSinkFunction<Tuple4<Long, Double, Float, Float>> {
        @Override
        public void process(Tuple4<Long, Double, Float, Float> tuple, RuntimeContext ctx, RequestIndexer indexer) {
            Map<String, String> json = new HashMap<>();
            json.put("time", tuple.f0.toString());
            json.put("power", tuple.f1.toString());
            json.put("location", tuple.f2+","+tuple.f3);

            IndexRequest rqst = Requests.indexRequest()
                    .index("gpx")
                    .type("power")
                    .source(json);
            indexer.add(rqst);
        }
    }
}

/*
curl -X DELETE "localhost:9200/gpx"


curl -X PUT "http://localhost:9200/gpx" -H 'Content-Type: application/json' -d'
{
    "settings" : {
        "number_of_shards" : 1
    },
    "mappings" : {
        "power" : {
            "properties" : {
                "time" : { "type" : "date", "format" : "epoch_second" },
                "power" : { "type" : "double" },
                "location" : { "type" : "geo_point" }
            }
        }
    }
}
'


 */