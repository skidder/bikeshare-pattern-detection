package com.github.skidder.gbfs.processor.sink;

import com.squareup.okhttp.ConnectionPool;
import com.squareup.okhttp.OkHttpClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import retrofit.client.OkClient;

import java.util.concurrent.TimeUnit;

public abstract class AbstractInfluxDBSink<IN> extends RichSinkFunction<IN> {
    private static final long serialVersionUID = -5303471202754594159L;

    protected transient OkHttpClient okHttpClient;
    protected transient InfluxDB influxDB;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        okHttpClient = new OkHttpClient();
        influxDB = InfluxDBFactory.connect("http://influxdb:8086", "gbfs", "gbfs", new OkClient(okHttpClient));

        // Flush every 2000 Points, at least every 100ms
        influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        // need to shutdown HTTP client thread pool to prevent orphaned threads
        // https://issues.apache.org/jira/browse/FLINK-4536
        if (okHttpClient != null) {
            okHttpClient.getDispatcher().getExecutorService().shutdown();
            final ConnectionPool connectionPool = okHttpClient.getConnectionPool();
            if (connectionPool != null) {
                connectionPool.evictAll();
            }
        }
    }
}
