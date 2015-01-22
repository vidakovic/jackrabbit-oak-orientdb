package org.apache.jackrabbit.oak.plugins.document.orient;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Striped;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.*;
import org.apache.jackrabbit.oak.plugins.document.cache.CachingDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.cache.ForwardingListener;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocOffHeapCache;
import org.apache.jackrabbit.oak.plugins.document.cache.OffHeapCache;
import org.apache.jackrabbit.oak.plugins.document.orient.util.OrientDocumentDatabaseFactory;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class OrientDocumentStore implements CachingDocumentStore {

    private static final Logger LOG = LoggerFactory.getLogger(OrientDocumentStore.class);

    private static final boolean LOG_TIME = false;

    /**
     * The sum of all milliseconds this class waited for OrientDB.
     */
    private long timeSum;

    private final Cache<CacheValue, NodeDocument> nodesCache;
    private final CacheStats cacheStats;

    /**
     * Locks to ensure cache consistency on reads, writes and invalidation.
     */
    private final Striped<Lock> locks = Striped.lock(128);

    /**
     * ReadWriteLocks to synchronize cache access when child documents are
     * requested from OrientDB and put into the cache. Accessing a single
     * document in the cache will acquire a read (shared) lock for the parent
     * key in addition to the lock (from {@link #locks}) for the individual
     * document. Reading multiple sibling documents will acquire a write
     * (exclusive) lock for the parent key. See OAK-1897.
     */
    private final Striped<ReadWriteLock> parentLocks = Striped.readWriteLock(64);

    /**
     * Comparator for maps with {@link org.apache.jackrabbit.oak.plugins.document.Revision} keys. The maps are ordered
     * descending, newest revisions first!
     */
    private final Comparator<Revision> comparator = StableRevisionComparator.REVERSE;

    private Clock clock = Clock.SIMPLE;

    private final long maxReplicationLagMillis;

    /**
     * Duration in seconds under which queries would use index on _modified field
     * If set to -1 then modifiedTime index would not be used
     */
    private final long maxDeltaForModTimeIdxSecs = Long.getLong("oak.orient.maxDeltaForModTimeIdxSecs",-1);

    private String lastReadWriteMode;

    private OrientDocumentDatabaseFactory dbf;

    public OrientDocumentStore(OrientDocumentDatabaseFactory dbf, DocumentMK.Builder builder) {
        checkVersion();

        this.dbf = dbf;

        maxReplicationLagMillis = builder.getMaxReplicationLagMillis();

        // TODO: create schema

        // indexes:
        // TODO: use schema to create indices

        // TODO expire entries if the parent was changed
        if (builder.useOffHeapCache()) {
            nodesCache = createOffHeapCache(builder);
        } else {
            nodesCache = builder.buildDocumentCache(this);
        }

        cacheStats = new CacheStats(nodesCache, "Document-Documents", builder.getWeigher(), builder.getDocumentCacheSize());
        LOG.info("Configuration maxReplicationLagMillis {}, " + "maxDeltaForModTimeIdxSecs {}",maxReplicationLagMillis, maxDeltaForModTimeIdxSecs);
    }

    private static void checkVersion() {
        String[] parts = OConstants.ORIENT_VERSION.split("\\.");

        if(Integer.valueOf(parts[0]) <2 ) {
            String msg = "OrientDB version 2.0 or higher required. " +
                    "Currently connected to a OrientDB with version: " + OConstants.ORIENT_VERSION;
            throw new RuntimeException(msg);
        }
    }

    private Cache<CacheValue, NodeDocument> createOffHeapCache(
            DocumentMK.Builder builder) {
        ForwardingListener<CacheValue, NodeDocument> listener = ForwardingListener.newInstance();

        Cache<CacheValue, NodeDocument> primaryCache = CacheBuilder.newBuilder()
                .weigher(builder.getWeigher())
                .maximumWeight(builder.getDocumentCacheSize())
                .removalListener(listener)
                .recordStats()
                .build();

        return new NodeDocOffHeapCache(primaryCache, listener, builder, this);
    }

    private static long start() {
        return LOG_TIME ? System.currentTimeMillis() : 0;
    }

    private void end(String message, long start) {
        if (LOG_TIME) {
            long t = System.currentTimeMillis() - start;
            if (t > 0) {
                LOG.debug(message + ": " + t);
            }
            timeSum += t;
        }
    }

    CachedNodeDocument getCachedNodeDoc(String id) {
        if (nodesCache instanceof OffHeapCache) {
            return ((OffHeapCache) nodesCache).getCachedDocument(id);
        }

        return nodesCache.getIfPresent(new StringValue(id));
    }

    @Override
    public void finalize() throws Throwable {
        super.finalize();
        // TODO should not be needed, but it seems
        // oak-jcr doesn't call dispose()
        dispose();
    }

    @Override
    public CacheStats getCacheStats() {
        return cacheStats;
    }

    long getMaxDeltaForModTimeIdxSecs() {
        return maxDeltaForModTimeIdxSecs;
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String s) {
        return null;
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String s, int i) {
        return null;
    }

    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String s, String s1, int i) {
        return null;
    }

    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String s, String s1, String s2, long l, int i) {
        return null;
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String s) {

    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> list) {

    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> list) {
        return false;
    }

    @Override
    public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {
        updateOp = updateOp.copy();

        long start = start();

        try {
            Map<String, NodeDocument> cachedDocs = Collections.emptyMap();
            if (collection == Collection.NODES) {
                cachedDocs = Maps.newHashMap();
                for (String key : keys) {
                    cachedDocs.put(key, nodesCache.getIfPresent(new StringValue(key)));
                }
            }
            try {
                // TODO: implement this
            } catch (OException e) {
                throw DocumentStoreException.convert(e);
            }
        } finally {
            end("update", start);
        }
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp updateOp) throws MicroKernelException {
        return null;
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp updateOp) throws MicroKernelException {
        return null;
    }

    @Override
    public void invalidateCache() {

    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
        if (collection == Collection.NODES) {
            ODocument doc = dbf.db().load(new ORecordId(key));
            doc.lock(true);
            try {
                nodesCache.invalidate(new StringValue(key));
            } finally {
                doc.unlock();
            }
        }
    }

    @Override
    public void dispose() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("OrientDB time: " + timeSum);
        }
        dbf.db().close();

        if (nodesCache instanceof Closeable) {
            try {
                ((Closeable) nodesCache).close();
            } catch (IOException e) {

                LOG.warn("Error occurred while closing Off Heap Cache", e);
            }
        }
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String s) {
        return null;
    }

    @Override
    public void setReadWriteMode(String s) {

    }

    protected <T extends Document> T convertFromODocument(Collection<T> collection, ODocument doc) {
        T copy = null;
        if (doc != null) {
            copy = collection.newDocument(this);
            for (String name : doc.fieldNames()) {
                Object o = doc.field(name);
                if (o instanceof String) {
                    copy.put(name, o);
                } else if (o instanceof Long) {
                    copy.put(name, o);
                } else if (o instanceof Integer) {
                    copy.put(name, o);
                } else if (o instanceof Boolean) {
                    copy.put(name, o);
                } else if (o instanceof ODocument) {
                    copy.put(name, doc.toMap());
                }
            }
        }
        return copy;
    }
}
