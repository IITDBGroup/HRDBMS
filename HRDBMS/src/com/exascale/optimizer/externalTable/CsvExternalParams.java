package com.exascale.optimizer.externalTable;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.HttpURLConnection;
import java.net.URL;

/** Properties for the CSV external table implementation */
public class CsvExternalParams implements ExternalParamsInterface {
    @JsonProperty
    private String delimiter = ",";
    @JsonProperty(required = true)
    private String url;
    @JsonProperty
    private boolean ignoreHeader;

    public CsvExternalParams() {}
    public void setDelimiter(String delimiter) { this.delimiter = delimiter; }
    public String getDelimiter() { return delimiter; }
    public void setUrl(String url) { this.url = url; }
    public String getUrl() { return url; }
    public void setIgnoreHeader(boolean ignoreHeader) { this.ignoreHeader = ignoreHeader; }
    public boolean getIgnoreHeader() { return ignoreHeader; }
    public boolean valid()
    {
        return checkHttpAddress(getUrl());
    }

    /** Check if HTTP address return 200 status code */
    private static boolean checkHttpAddress(String csvFile) {
        if (csvFile == null) {
            throw new RuntimeException("URL is not defined!");
        }
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
