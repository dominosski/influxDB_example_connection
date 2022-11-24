package org.example;

import com.influxdb.client.InfluxDBClient;

public class Main {
    public static void main(String[] args) {
        String apiToken = "INFLUX_API_KEY";
        String bucket = "bucket_name";
        String organization = "org_name";
        String url = "http://localhost:8086";

        InfluxDBConnection influxDBConnection = new InfluxDBConnection();
        InfluxDBClient influxDBClient = influxDBConnection.buildConnection(apiToken, bucket, organization, url);

        boolean resultSingle = influxDBConnection.singlePointWrite(influxDBClient);
        if(resultSingle){
            System.out.println("Single point write finished");
        }

        boolean resultMultiple = influxDBConnection.multiplePointsWrite(influxDBClient);
        if(resultMultiple){
            System.out.println("Multiple point write finished");
        }

        boolean resultPOJO = influxDBConnection.writePointByPOJO(influxDBClient);
        if(resultPOJO){
            System.out.println("POJO point write finished");
        }

        influxDBConnection.queryData(influxDBClient);

        if(influxDBConnection.deleteRecord(influxDBClient)){
            System.out.println("Delete record finished");
        }

        influxDBClient.close();
    }
}