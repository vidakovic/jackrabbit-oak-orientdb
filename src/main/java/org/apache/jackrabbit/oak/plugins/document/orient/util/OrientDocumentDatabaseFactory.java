package org.apache.jackrabbit.oak.plugins.document.orient.util;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrientDocumentDatabaseFactory {

    private static Logger log = LoggerFactory.getLogger(OrientDocumentDatabaseFactory.class);

    public static final String DEFAULT_USERNAME = "admin";

    public static final String DEFAULT_PASSWORD = "admin";

    public static final int DEFAULT_MAX_POOL_SIZE = 20;

    protected String username = DEFAULT_USERNAME;

    protected String password = DEFAULT_PASSWORD;

    protected int maxPoolSize = DEFAULT_MAX_POOL_SIZE;

    protected Boolean autoCreate;

    protected String url;

    private OPartitionedDatabasePool pool;

    private ODatabaseDocumentTx db;

    public OrientDocumentDatabaseFactory() {
        this("plocal:/tmp/jackrabbit", "admin", "admin", DEFAULT_MAX_POOL_SIZE);
    }

    public OrientDocumentDatabaseFactory(String url, String username, String password, int maxPoolSize) {

    }

    protected void createPool() {
        pool = new OPartitionedDatabasePool(getUrl(), getUsername(), getPassword(), maxPoolSize);

        assert url!=null;
        assert username!=null;
        assert password!=null;

        if(autoCreate==null) {
            autoCreate = !getUrl().startsWith("remote:");
        }

        ODatabase<?> db = newDatabase();
        createDatabase(db);
        createPool();
    }

    public ODatabaseDocumentTx openDatabase() {
        db = pool.acquire();
        return db;
    }

    protected ODatabaseDocumentTx newDatabase() {
        return new ODatabaseDocumentTx(getUrl());
    }

    public ODatabaseDocumentTx db() {
        ODatabaseDocumentTx db;
        if(!ODatabaseRecordThreadLocal.INSTANCE.isDefined()) {
            db = openDatabase();
            log.debug("acquire db from pool {}", db.hashCode());
        } else {
            db = (ODatabaseDocumentTx)ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner();

            if(db.isClosed()) {
                db = openDatabase();
                log.debug("re-opened db {}", db.hashCode());
            } else {
                log.debug("use existing db {}", db.hashCode());
            }
        }

        return db;
    }

    protected void createDatabase(ODatabase<?> db) {
        if (autoCreate) {
            if (!db.exists()) {
                db.create();
                db.close();
            }
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public Boolean getAutoCreate() {
        return autoCreate;
    }

    public void setAutoCreate(Boolean autoCreate) {
        this.autoCreate = autoCreate;
    }
}   
