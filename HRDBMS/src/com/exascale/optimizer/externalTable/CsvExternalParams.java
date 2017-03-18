package com.exascale.optimizer.externalTable;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/** Properties for the CSV external table implementation */
public class CsvExternalParams implements ExternalParamsInterface {
    public enum Source{URL, HDFS};
    @JsonProperty
    private Source source = Source.URL;
    @JsonProperty
    private String delimiter = ",";
    @JsonProperty(required = true)
    private String location;
    @JsonProperty
    private boolean ignoreHeader;

    public CsvExternalParams() {}
    public void setSource(String source) { this.source = Source.valueOf(source); }
    public Source getSource() { return source; }
    public void setDelimiter(String delimiter) { this.delimiter = delimiter; }
    public String getDelimiter() { return delimiter; }
    public void setLocation(String location) { this.location = location; }
    public String getLocation() { return location; }
    public void setIgnoreHeader(boolean ignoreHeader) { this.ignoreHeader = ignoreHeader; }
    public boolean getIgnoreHeader() { return ignoreHeader; }
    public boolean valid()
    {
        if (this.location == null) {
            throw new RuntimeException("File location is not specified!");
        }
        if (this.source == Source.URL) {
            return checkHttpAddress(this.location);
        } else if (this.source == Source.HDFS) {
            return fileExists(this.location);
        } else {
            return false;
        }
    }

    /** Check if file exists in HDFS */
    private static boolean fileExists(String fileName) {
        try{
            // TODO this code does work becuase of Hadoop library dependecies conflicts, etc
            Path pt=new Path(fileName);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();
            while (line != null){
                System.out.println(line);
                line=br.readLine();
            }
        }catch(Exception e){
            String mess = e.getMessage();
            e.printStackTrace();
            StackTraceElement[] st = e.getStackTrace();
        }
        return true;
    }

    /** Check if HTTP address return 200 status code */
    private static boolean checkHttpAddress(String csvFile) {
        try {
            URL u = new URL(csvFile);
            HttpURLConnection huc = (HttpURLConnection) u.openConnection();
            huc.setRequestMethod("HEAD");
            huc.connect();
            if (HttpURLConnection.HTTP_OK == huc.getResponseCode()) {
                return true;
            }
        } catch (Exception e) {

        }
        throw new RuntimeException("URL '" + csvFile + "' does not respond");
    }

}
