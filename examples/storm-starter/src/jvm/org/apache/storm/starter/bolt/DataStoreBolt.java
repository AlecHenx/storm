package org.apache.storm.starter.bolt;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.DaemonConfig;
import org.apache.storm.metricstore.rocksdb.RocksDbStore;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.trajstore.TrajPoint;
import org.apache.storm.trajstore.TrajStore;
import org.apache.storm.trajstore.TrajStoreConfig;
import org.apache.storm.trajstore.TrajStoreException;
import org.apache.storm.trajstore.rocksdb.StringMetadataCache;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// "trajId", "edgeId", "dist"
public class DataStoreBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(DataStoreBolt.class);
    private PrintWriter writer;
    private Map<String, Object> stormConf;
    private TopologyContext context;
    private TrajStore store;
    private Path tempDirForStore;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.stormConf = stormConf;
        this.context = context;
        try {
            StringMetadataCache.cleanUp();
            tempDirForStore = Path.of("C:\\Users\\ASUS\\AppData\\Local\\Temp\\RocksDbStoreTest");
            Map<String, Object> conf = new HashMap<>();
            conf.put(DaemonConfig.STORM_METRIC_STORE_CLASS, "org.apache.storm.trajstore.rocksdb.RocksDbStore");
            conf.put(DaemonConfig.STORM_ROCKSDB_LOCATION, tempDirForStore.toString());
            conf.put(DaemonConfig.STORM_ROCKSDB_CREATE_IF_MISSING, true);
            conf.put(DaemonConfig.STORM_ROCKSDB_METADATA_STRING_CACHE_CAPACITY, 4000);
            conf.put(DaemonConfig.STORM_ROCKSDB_METRIC_RETENTION_HOURS, 240);
            store = TrajStoreConfig.configure(conf);

        } catch (TrajStoreException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Integer trajId = input.getIntegerByField("trajId");
        Long timestamp = input.getLongByField("timestamp");
        Long edgeId = input.getLongByField("edgeId");
        Double dist = input.getDoubleByField("dist");

        TrajPoint p = new TrajPoint(trajId, timestamp, edgeId, dist);
        try {
            store.insert(p);
        } catch (TrajStoreException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    @Override
    public void cleanup() {
        if (store != null) {
            store.close();
        }
        StringMetadataCache.cleanUp();
    }

}
