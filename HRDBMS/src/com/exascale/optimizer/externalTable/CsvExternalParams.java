package com.exascale.optimizer.externalTable;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Properties for the CSV external table implementation */
public class CsvExternalParams {
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
}
