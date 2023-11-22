/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.trajstore.rocksdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.esotericsoftware.minlog.Log;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.storm.DaemonConfig;
import org.apache.storm.trajstore.TrajStoreException;
import org.apache.storm.trajstore.TrajPoint;
import org.apache.storm.trajstore.TrajStore;
import org.apache.storm.trajstore.TrajStoreConfig;
import org.apache.storm.trajstore.FilterOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RocksDbStoreTest {
    static TrajStore store;
    static Path tempDirForTest;

    @BeforeAll
    public static void setUp() throws TrajStoreException, IOException {
        // remove any previously created cache instance
        StringMetadataCache.cleanUp();
        tempDirForTest = Files.createTempDirectory("RocksDbStoreTest");
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.STORM_METRIC_STORE_CLASS, "org.apache.storm.trajstore.rocksdb.RocksDbStore");
        conf.put(DaemonConfig.STORM_ROCKSDB_LOCATION, tempDirForTest.toString());
        conf.put(DaemonConfig.STORM_ROCKSDB_CREATE_IF_MISSING, true);
        conf.put(DaemonConfig.STORM_ROCKSDB_METADATA_STRING_CACHE_CAPACITY, 4000);
        conf.put(DaemonConfig.STORM_ROCKSDB_METRIC_RETENTION_HOURS, 240);
        store = TrajStoreConfig.configure(conf);
    }

    @AfterAll
    public static void tearDown() throws IOException {
        if (store != null) {
            store.close();
        }
        StringMetadataCache.cleanUp();
//        FileUtils.deleteDirectory(tempDirForTest.toFile());
    }

    @Test
    public void basicInsertAndLookup() throws Exception {
        TrajPoint toPopulate = null;
        for (int i = 0; i < 20; i++) {
            Long value = 5L + i;
            long timestamp = 1L + i * 60 * 1000;
            TrajPoint m = new TrajPoint(1, timestamp, value, value * 0.01);
            toPopulate = new TrajPoint(m);
            store.insert(m);
            // Indicate to test rocksdb lsm tree (newer key will override old key
            if (i == 0) {
                m = new TrajPoint(1, timestamp, value + 10, (value + 10) * 0.01);
                toPopulate = new TrajPoint(m);
                store.insert(m);
            }
        }

        waitForInsertFinish(toPopulate);

        // basic
        toPopulate.setTrajId(1);
        toPopulate.setTimestamp(1L + 60 * 1000);
        boolean res = store.populateValue(toPopulate);
        assertTrue(res);
        assertEquals(6, toPopulate.getEdgeId(), 0.001);

        // re-lookup again
        res = store.populateValue(toPopulate);
        assertTrue(res);
        assertEquals(6, toPopulate.getEdgeId(), 0.001);

        // We can't find non-existed key
        toPopulate.setTrajId(2);
        toPopulate.setTimestamp(1L + 60 * 1000);
        res = store.populateValue(toPopulate);
        assertFalse(res);

        // override
        toPopulate.setTrajId(1);
        toPopulate.setTimestamp(1L);
        res = store.populateValue(toPopulate);
        assertTrue(res);
        assertEquals(15, toPopulate.getEdgeId(), 0.001);

    }

    @Test
    public void MultiTrajectoryStore() throws Exception {
        TrajPoint toPopulate = null;
        for (int i = 0; i < 20; i++) {
            Long value = 5L + i;
            long timestamp = 1L + i * 60 * 1000;
            TrajPoint m1 = new TrajPoint(3, timestamp, value, value * 0.01);
            TrajPoint m2 = new TrajPoint(4, timestamp, value, value * 0.01);
            toPopulate = new TrajPoint(m1);
            store.insert(toPopulate);
            toPopulate = new TrajPoint(m2);
            store.insert(toPopulate);
        }

        waitForInsertFinish(toPopulate);

        // Query traj3
        toPopulate.setTrajId(3);
        toPopulate.setTimestamp(1L + 60 * 1000);
        boolean res = store.populateValue(toPopulate);
        assertTrue(res);
        assertEquals(6, toPopulate.getEdgeId(), 0.001);
        assertEquals(0.06, toPopulate.getDistance(), 0.001);

        // Query traj4
        toPopulate.setTrajId(4);
        toPopulate.setTimestamp(1L);
        waitForInsertFinish(toPopulate);
        res = store.populateValue(toPopulate);
        // 因为这几个test是并发运行的，而使用的却是同一个store，所以此处查询时可以别处有被修改了。因此直接改成3和4轨迹。
        assertTrue(res);
        Log.info(toPopulate.getTrajId().toString());
        assertEquals(5, toPopulate.getEdgeId(), 0.001);
        assertEquals(0.05, toPopulate.getDistance(), 0.001);

    }

    private List<TrajPoint> getMetricsFromScan(FilterOptions filter) throws TrajStoreException {
        List<TrajPoint> list = new ArrayList<>();
        store.scan(filter, list::add);
        return list;
    }

    //    @Test
    public void RangeScanByTime() throws Exception {
        FilterOptions filter;
        TrajPoint toPopulate = null;
        List<TrajPoint> list;
        for (int i = 0; i < 200; i++) {
            Long value = 5L + i;
            long timestamp = 1L + i * 60 * 1000;
            TrajPoint m1 = new TrajPoint(5, timestamp, value, value * 0.01);
            TrajPoint m2 = new TrajPoint(6, timestamp + 10 * 60 * 1000, value, value * 0.01);
            toPopulate = new TrajPoint(m1);
            store.insert(toPopulate);
            toPopulate = new TrajPoint(m2);
            store.insert(toPopulate);
        }
        waitForInsertFinish(toPopulate);

        // validate search by time
        filter = new FilterOptions();
//        filter.setTrajectoryId(1);
        filter.setStartTime(1L + 10 * 60 * 1000);
        filter.setEndTime(1L + 30 * 60 * 1000 + 1);
        list = getMetricsFromScan(filter);
        assertEquals(21 * 2, list.size());
        Set<Integer> trajIds = new HashSet<>();
        for (TrajPoint p : list) {
            trajIds.add(p.getTrajId());
        }
        assertTrue(trajIds.contains(5));
        assertTrue(trajIds.contains(6));

        // validate search by time
        filter = new FilterOptions();
        filter.setStartTime(1L);
        filter.setEndTime(1L + 5 * 60 * 1000 + 1);
        list = getMetricsFromScan(filter);
        assertEquals(6, list.size());
        trajIds = new HashSet<>();
        for (TrajPoint p : list) {
            trajIds.add(p.getTrajId());
        }
        assertTrue(trajIds.contains(5));
        assertFalse(trajIds.contains(6));
    }

    @Test
    public void IDQuery() throws Exception {
        FilterOptions filter;
        TrajPoint toPopulate = null;
        List<TrajPoint> list;
        for (int i = 0; i < 200; i++) {
            Long value = 5L + i;
            long timestamp = 1L + i * 60 * 1000;
            TrajPoint m1 = new TrajPoint(7, timestamp, value, value * 0.01);
            TrajPoint m2 = new TrajPoint(8, timestamp + 10 * 60 * 1000, value, value * 0.01);
            toPopulate = new TrajPoint(m1);
            store.insert(toPopulate);
            toPopulate = new TrajPoint(m2);
            store.insert(toPopulate);
        }
        waitForInsertFinish(toPopulate);

        // validate search by id
        filter = new FilterOptions();
        filter.setTrajectoryId(7);
        list = getMetricsFromScan(filter);
        assertEquals(200, list.size());
        Set<Integer> trajIds = new HashSet<>();
        for (TrajPoint p : list) {
            trajIds.add(p.getTrajId());
        }
        assertTrue(trajIds.contains(7));
        assertFalse(trajIds.contains(8));
    }

    private void waitForInsertFinish(TrajPoint m) throws Exception {
        TrajPoint last = new TrajPoint(m);
        int attempts = 0;
        do {
            Thread.sleep(1);
            attempts++;
            if (attempts > 5000) {
                throw new Exception("Insertion timing out");
            }
        } while (!store.populateValue(last));
    }
}
