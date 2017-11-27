package com.github.skidder.gbfs.processor.sink;

import com.github.skidder.gbfs.proto.Gbfs;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.influxdb.dto.Point;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GBFSStationStatusInfluxDBSink extends AbstractInfluxDBSink<Gbfs.CompleteGBFSMessage>
        implements SinkFunction<Gbfs.CompleteGBFSMessage> {
    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.flink.streaming.api.functions.sink.SinkFunction#invoke(java.
     * lang.Object)
     */
    @Override
    public void invoke(Gbfs.CompleteGBFSMessage t) throws Exception {
        Map<String, String> stationNameMap = new HashMap<>();
        for (Gbfs.StationInformationMessage.Station stationInfo : t.getStationInformationMessage().getData().getStationsList()) {
            stationNameMap.put(stationInfo.getStationId(), stationInfo.getName());
        }

        for (Gbfs.StationStatusMessage.Station station : t.getStationStatusMessage().getData().getStationsList()) {
            final String stationName = stationNameMap.get(station.getStationId());
            if (stationName == null || station.getLastReportedTimestamp() == null || station.getLastReported() == 0) {
                // ignore stations that don't have a name in the station information
                continue;
            }

            final Point.Builder builder = Point
                    .measurement("gbfs_station")
                    .time(station.getLastReportedTimestamp().getSeconds(), TimeUnit.SECONDS)
                    .tag("id", station.getStationId())
                    .tag("name", stationName)
                    .addField("num_bikes_available", station.getNumBikesAvailable())
                    .addField("num_bikes_disabled", station.getNumBikesDisabled())
                    .addField("is_installed", station.getIsInstalled())
                    .addField("is_renting", station.getIsRenting())
                    .addField("is_returning", station.getIsReturning())
                    .addField("num_docks_available", station.getNumDocksAvailable())
                    .addField("num_docks_disabled", station.getNumDocksDisabled());

            final Point point = builder.build();
            influxDB.write("gbfs", "gbfs_default", point);
        }
    }
}