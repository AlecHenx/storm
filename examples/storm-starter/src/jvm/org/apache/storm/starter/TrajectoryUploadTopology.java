/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.starter;

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.starter.bolt.DataStoreBolt;
import org.apache.storm.starter.bolt.MapMatchBolt;
import org.apache.storm.starter.bolt.WordCountBolt;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.starter.spout.RandomTrajectorySpout;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class TrajectoryUploadTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomTrajectorySpout(), 1);
        builder.setBolt("mapmatch", new MapMatchBolt(), 2).fieldsGrouping("spout", new Fields("trajId"));
        builder.setBolt("dataStore", new DataStoreBolt(), 1).fieldsGrouping("mapmatch", new Fields("trajId"));

        Config config = new Config();
        config.setDebug(false);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("trajectoryUpload", config, builder.createTopology());
    }

}
