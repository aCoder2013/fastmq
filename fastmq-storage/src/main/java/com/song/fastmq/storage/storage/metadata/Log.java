package com.song.fastmq.storage.storage.metadata;

import java.util.List;

/**
 * Created by song on 2017/11/5.
 */
public class Log {

    /**
     * Name of this log
     */
    private String name;

    private List<LogSegment> segments;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<LogSegment> getSegments() {
        return segments;
    }

    public void setSegments(List<LogSegment> segments) {
        this.segments = segments;
    }
}
