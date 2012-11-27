package org.elasticsearch.river.jdbc;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-25
 * Time: 19:49
 */
public class VersionDigest {
    private Number version;
    private String digest;

    public VersionDigest(Number version, String digest) {
        this.version = version;
        this.digest = digest;
    }

    public long getVersion() {
        return version.longValue();
    }

    public void setVersion(Number version) {
        this.version = version;
    }

    public String getDigest() {
        return digest;
    }

    public void setDigest(String digest) {
        this.digest = digest;
    }
}
