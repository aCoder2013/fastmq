package com.song.fastmq.storage.storage;

/**
 * @author song
 */
public class LogReaderInfo {

    private String logName;

    private String logReaderName;

    public LogReaderInfo() {
    }

    public LogReaderInfo(String logName, String logReaderName) {
        this.logName = logName;
        this.logReaderName = logReaderName;
    }

    public String getLogName() {
        return logName;
    }

    public void setLogName(String logName) {
        this.logName = logName;
    }

    public String getLogReaderName() {
        return logReaderName;
    }

    public void setLogReaderName(String logReaderName) {
        this.logReaderName = logReaderName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LogReaderInfo that = (LogReaderInfo) o;

        if (!logName.equals(that.logName)) {
            return false;
        }
        return logReaderName.equals(that.logReaderName);
    }

    @Override
    public int hashCode() {
        int result = logName.hashCode();
        result = 31 * result + logReaderName.hashCode();
        return result;
    }
}
