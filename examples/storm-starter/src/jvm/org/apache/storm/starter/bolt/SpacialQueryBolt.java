package org.apache.storm.starter.bolt;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.agrona.collections.LongArrayList;
import org.apache.storm.Config;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.datanucleus.store.types.wrappers.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author alecHe
 * @desc ...
 * @date 2023-11-28 10:57:52
 */
public class SpacialQueryBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(IndexStoreBolt.class);
    private Map<String, Object> stormConf;
    private TopologyContext context;
    // 轨迹id的最大值
    private static Integer MAX_TRAJID_NUM = 1000;
    LocalFsBlobStore store = null;

    public void prepare(Map stormConf, TopologyContext context) {
        this.stormConf = stormConf;
        this.context = context;
        this.store = initLocalFs();
    }

    private LocalFsBlobStore initLocalFs() {
        LocalFsBlobStore store = new LocalFsBlobStore();

        Map<String, Object> conf = Utils.readStormConfig();
        conf.put(Config.STORM_ZOOKEEPER_PORT, this.stormConf.get("storm.zookeeper.port"));
        conf.put(Config.STORM_LOCAL_DIR, "C:\\Users\\ASUS\\AppData\\Local\\Temp\\IndexStoreTest");
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, "org.apache.storm.security.auth.DefaultPrincipalToLocal");
        NimbusInfo nimbusInfo = new NimbusInfo("localhost", 0, false);
        store.prepare(conf, null, nimbusInfo, null);
        return store;
    }

    Set<Integer> deserialize(byte[] values) {
        Set<Integer> sets = new HashSet<>();
        int length = ByteBuffer.wrap(values, 0, 4).getInt();
        for (int i = 0; i < length; ++i) {
            int v = ByteBuffer.wrap(values, 4 + i * 4, 4).getInt();
            sets.add(v);
        }
        return sets;
    }

    private Set<Integer> lookup(Long edgeId)
        throws IOException, AuthorizationException {
        // 1. read
        byte[] out = new byte[MAX_TRAJID_NUM * 4];
        Set<Integer> old = null;
        try (InputStream in = store.getBlob(edgeId.toString(), null)) {
            in.read(out);
            // 4.update
            old = deserialize(out);
            // 2. check
        } catch (KeyNotFoundException e) {
            // No this key, insert case.
            LOG.info("No this key {}", edgeId);
        }
        return old;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (tuple == null || !tuple.contains("SpacialRange")) {
            return;
        }
        List<Long> edgeIds = (LongArrayList) tuple.getValue(1);
        Set<Integer> trajs = new HashSet<>();
        for (Long edgeId : edgeIds) {
            try {
                Set<Integer> tmp = lookup(edgeId);
                trajs.addAll(tmp);
            } catch (Exception r) {
                r.printStackTrace();
            }
        }
        for (Integer trajId : trajs) {
            collector.emit(new Values(trajId,
                tuple.getLongByField("startTime"), tuple.getLongByField("endTime")));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("trajId", "startTime", "endTime"));
    }
}