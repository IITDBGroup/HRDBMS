package com.exascale.optimizer.externalTable;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Properties for the CSV external table implementation */
public class CsvExternalParams {
    @JsonProperty(required = true)
    private String delimiter;
    @JsonProperty(required = true)
    private String url;

    public CsvExternalParams() {}
    public void setDelimiter(String delimiter) { this.delimiter = delimiter; }
    public String getDelimiter() { return delimiter; }
    public void setUrl(String url) { this.url = url; }
    public String getUrl() { return url; }
}
