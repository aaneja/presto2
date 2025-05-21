package com.facebook.presto.spi.router;

import javax.servlet.http.HttpServletRequest;

public class RequestInfo {
    private final String query;
    private final HttpServletRequest servletRequest;

    public RequestInfo(String query, HttpServletRequest servletRequest) {
        this.query = query;
        this.servletRequest = servletRequest;
    }

    public String getQuery()
    {
        return query;
    }

    public HttpServletRequest getServletRequest()
    {
        return servletRequest;
    }
}