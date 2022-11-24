package org.example;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.*;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.exceptions.InfluxException;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class InfluxDBConnection {
    private String token;
    private String bucket;
    private String organization;
    private String url;

    public InfluxDBClient buildConnection(String token, String bucket, String organization, String url) {
        setToken(token);
        setBucket(bucket);
        setOrganization(organization);
        setUrl(url);
        return InfluxDBClientFactory.create(getUrl(), getToken().toCharArray(), getOrganization(), getBucket());
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public boolean singlePointWrite(InfluxDBClient influxDBClient){
        boolean flag = false;
        WriteApiBlocking writeApiBlocking = influxDBClient.getWriteApiBlocking();

       try{
           Point point = Point.measurement("sensor")
                   .addTag("sensor_id", "sensor001")
                   .addField("location", "City center")
                   .addField("model_number", "TLM890123")
                   .time(Instant.parse("2022-11-23T13:19:00.117484Z"), WritePrecision.MS);

           writeApiBlocking.writePoint(point);
           flag = true;
       }catch (InfluxException x){
           x.printStackTrace();
       }
       return flag;
    }

    public boolean multiplePointsWrite(InfluxDBClient influxDBClient){
        boolean flag = false;
        try {
            WriteApiBlocking writeApiBlocking = influxDBClient.getWriteApiBlocking();

            Point point1 = Point.measurement("sensor")
                    .addTag("sensor_id", "sensor002")
                    .addField("location", "Manhattan")
                    .addField("model_number", "TLM890123")
                    .time(Instant.parse("2022-11-23T13:17:00.117484Z"), WritePrecision.MS);
            Point point2 = Point.measurement("sensor")
                    .addTag("sensor_id", "sensor003")
                    .addField("location", "Bronx")
                    .addField("model_number", "TLM890125")
                    .time(Instant.parse("2022-11-23T13:16:00.117484Z"), WritePrecision.MS);
            Point point3 = Point.measurement("sensor")
                    .addTag("sensor_id", "sensor004")
                    .addField("location", "Brooklyn")
                    .addField("model_number", "TLM892325")
                    .time(Instant.parse("2022-11-23T13:15:00.117484Z"), WritePrecision.MS);

            List<Point> pointsList = new ArrayList<>();

            pointsList.add(point1);
            pointsList.add(point2);
            pointsList.add(point3);

            writeApiBlocking.writePoints(pointsList);

            flag = true;
        }catch (InfluxException x){
            x.printStackTrace();
        }
        return flag;
    }

    public boolean writePointByPOJO(InfluxDBClient influxDBClient){
        boolean flag = false;
        try {
            WriteApiBlocking writeApiBlocking = influxDBClient.getWriteApiBlocking();

            Sensor sensor = new Sensor();
            sensor.sensor_id = "sensor005";
            sensor.location = "Long Island";
            sensor.model_number = "TLM31223245";
            sensor.last_inspected = Instant.parse("2022-11-23T13:13:00.117484Z");
            writeApiBlocking.writeMeasurement(WritePrecision.MS, sensor);
            flag = true;
        }catch (InfluxException x){
            x.printStackTrace();
        }
        return flag;
    }

    @Measurement(name = "sensor")
    private static class Sensor {
        @Column(tag = true)
        String sensor_id;
        @Column
        String location;
        @Column
        String model_number;
        @Column(timestamp = true)
        Instant last_inspected;
    }

    public void queryData(InfluxDBClient influxDBClient){
        String flux = "from(bucket:\"Mydb\") |> range(start:0) |> filter(fn: (r) => " +
                "r[\"_measurement\"] == \"sensor\")";

        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> fluxTables = queryApi.query(flux);
        for(FluxTable fluxTable : fluxTables){
            List<FluxRecord> fluxRecords = fluxTable.getRecords();
            for(FluxRecord fluxRecord : fluxRecords){
                System.out.println(fluxRecord.getValueByKey("sensor_id"));
            }
        }
    }
    public boolean deleteRecord(InfluxDBClient influxDBClient){
        boolean flag = false;
        DeleteApi deleteApi = influxDBClient.getDeleteApi();
        try {
            OffsetDateTime start = OffsetDateTime.now().minus(72, ChronoUnit.HOURS);
            OffsetDateTime stop = OffsetDateTime.now();
            String predicate = "_Measurement=\"sensor\" AND sensor_id = \"sensor004\"";

            deleteApi.delete(start, start, predicate, "Mydb", "None");
            flag = true;
        }catch (InfluxException x){
            x.printStackTrace();
        }
        return flag;
    }
}
