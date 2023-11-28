/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.starter.spout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.starter.mapmatch.types.GpsMeasurement;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;

public class RandomTrajectorySpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(RandomTrajectorySpout.class);
    SpoutOutputCollector collector;
    Integer pointer = 0;
    Integer m = 1;
    Integer trajIdPointer = 0;
    String[] list;
    List<Values> values;
    Integer maxTrajectoryIndex = 1000;

    private static Date seconds(int seconds) {
        Calendar c = new GregorianCalendar(2014, 1, 1);
        c.add(Calendar.SECOND, seconds);
        return c.getTime();
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        File path = new File("F:\\File-D\\mapOut");
        list = path.list();
        values = new ArrayList<>();
        readFromFile();
        LOG.info("read file done");
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        if (pointer <= maxTrajectoryIndex) {
            collector.emit(values.get(pointer++));
        } else {
            LOG.info("spout is done.");
        }
//        GpsMeasurement gps1 = new GpsMeasurement(seconds(0), 10, 10);
//        GpsMeasurement gps2 = new GpsMeasurement(seconds(1), 30, 20);
//        GpsMeasurement gps3 = new GpsMeasurement(seconds(2), 30, 40);
//        GpsMeasurement gps4 = new GpsMeasurement(seconds(3), 10, 70);
//        List<GpsMeasurement> gpsMeasurements = Arrays.asList(gps1, gps2, gps3, gps4);
//
//        pointer++;
//        final GpsMeasurement point = gpsMeasurements.get(pointer % gpsMeasurements.size());
//
//        LOG.info("Emitting tuple: {}-{}", pointer / gpsMeasurements.size(), point);
//
//        collector.emit(new Values(pointer / gpsMeasurements.size(), pointer % gpsMeasurements.size(),
//            point.position.x, point.position.y));
    }

    public void readFromFile() {
        try {
            for (String file : list) {
                LOG.info("Trajectory id: {}", file);
                BufferedReader in = new BufferedReader(new FileReader("F:\\File-D\\mapOut\\" + file));
                String str;
                while ((str = in.readLine()) != null) {
                    String[] edges = str.split(",");
                    Long timestamp = 0L;
                    for (String edgeStr : edges) {
                        int trajId = Integer.parseInt(file);
                        long edge = Long.parseLong(edgeStr);
                        values.add(new Values(trajId, timestamp, edge, 0.0));
                        timestamp++;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("trajId", "timestamp", "lat", "lng"));
        declarer.declare(new Fields("trajId", "timestamp", "edgeId", "dist"));
    }

}
