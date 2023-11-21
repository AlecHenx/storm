package org.apache.storm.starter.bolt;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.starter.hmm.SequenceState;
import org.apache.storm.starter.hmm.Transition;
import org.apache.storm.starter.hmm.ViterbiAlgorithm;
import org.apache.storm.starter.mapmatch.HmmProbabilities;
import org.apache.storm.starter.mapmatch.TimeStep;
import org.apache.storm.starter.mapmatch.types.GpsMeasurement;
import org.apache.storm.starter.mapmatch.types.Point;
import org.apache.storm.starter.mapmatch.types.RoadPath;
import org.apache.storm.starter.mapmatch.types.RoadPosition;
import org.apache.storm.starter.spout.RandomTrajectorySpout;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jline.utils.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapMatchBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MapMatchBolt.class);
    private Integer bufferSize = 10;
    // Buffer
    private List<GpsMeasurement> gpsMeasurements;
    private TimeStep<RoadPosition, GpsMeasurement, RoadPath> prevTimeStep = null;
    ViterbiAlgorithm<RoadPosition, GpsMeasurement, RoadPath> viterbi = null;
    private HmmProbabilities hmmProbabilities = null;
    private static Map<Integer, Collection<RoadPosition>> candidateMap = null;
    private static Map<Transition<RoadPosition>, Double> routeLengths = null;

    private static RoadPosition rp11 = null;
    private static RoadPosition rp12 = null;
    private static RoadPosition rp21 = null;
    private static RoadPosition rp22 = null;
    private static RoadPosition rp31 = null;
    private static RoadPosition rp32 = null;
    private static RoadPosition rp33 = null;
    private static RoadPosition rp41 = null;
    private static RoadPosition rp42 = null;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        super.prepare(topoConf, context);
        hmmProbabilities = new HmmProbabilities();
        candidateMap = new HashMap<>();
        routeLengths = new HashMap<>();
        viterbi = new ViterbiAlgorithm<>();
        rp11 = new RoadPosition(1, 1.0 / 5.0, 20.0, 10.0);
        rp12 = new RoadPosition(2, 1.0 / 5.0, 60.0, 10.0);
        rp21 = new RoadPosition(1, 2.0 / 5.0, 20.0, 20.0);
        rp22 = new RoadPosition(2, 2.0 / 5.0, 60.0, 20.0);
        rp31 = new RoadPosition(1, 5.0 / 6.0, 20.0, 40.0);
        rp32 = new RoadPosition(3, 1.0 / 4.0, 30.0, 50.0);
        rp33 = new RoadPosition(2, 5.0 / 6.0, 60.0, 40.0);
        rp41 = new RoadPosition(4, 2.0 / 3.0, 20.0, 70.0);
        rp42 = new RoadPosition(5, 2.0 / 3.0, 60.0, 70.0);
        // Fake gps
        GpsMeasurement gps1 = new GpsMeasurement(seconds(0), 10, 10);
        GpsMeasurement gps2 = new GpsMeasurement(seconds(1), 30, 20);
        GpsMeasurement gps3 = new GpsMeasurement(seconds(2), 30, 40);
        GpsMeasurement gps4 = new GpsMeasurement(seconds(3), 10, 70);

        candidateMap.put(0, Arrays.asList(rp11, rp12));
        candidateMap.put(1, Arrays.asList(rp21, rp22));
        candidateMap.put(2, Arrays.asList(rp31, rp32, rp33));
        candidateMap.put(3, Arrays.asList(rp41, rp42));

        addRouteLength(rp11, rp21, 10.0);
        addRouteLength(rp11, rp22, 110.0);
        addRouteLength(rp12, rp21, 110.0);
        addRouteLength(rp12, rp22, 10.0);

        addRouteLength(rp21, rp31, 20.0);
        addRouteLength(rp21, rp32, 40.0);
        addRouteLength(rp21, rp33, 80.0);
        addRouteLength(rp22, rp31, 80.0);
        addRouteLength(rp22, rp32, 60.0);
        addRouteLength(rp22, rp33, 20.0);

        addRouteLength(rp31, rp41, 30.0);
        addRouteLength(rp31, rp42, 70.0);
        addRouteLength(rp32, rp41, 30.0);
        addRouteLength(rp32, rp42, 50.0);
        addRouteLength(rp33, rp41, 70.0);
        addRouteLength(rp33, rp42, 30.0);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("trajId", "edgeId", "dist"));
    }

    private static Date seconds(int seconds) {
        Calendar c = new GregorianCalendar(2014, 1, 1);
        c.add(Calendar.SECOND, seconds);
        return c.getTime();
    }

    private static void addRouteLength(RoadPosition from, RoadPosition to, double routeLength) {
        routeLengths.put(new Transition<RoadPosition>(from, to), routeLength);
    }

    /*
     * Returns the Cartesian distance between two points.
     * For real map matching applications, one would compute the great circle distance between
     * two GPS points.
     */
    private double computeDistance(Point p1, Point p2) {
        final double xDiff = p1.x - p2.x;
        final double yDiff = p1.y - p2.y;
        return Math.sqrt(xDiff * xDiff + yDiff * yDiff);
    }

    /*
     * For real map matching applications, candidates would be computed using a radius query.
     */
    private Collection<RoadPosition> computeCandidates(Integer id) {
        return candidateMap.get(id);
    }

    private void computeEmissionProbabilities(
        TimeStep<RoadPosition, GpsMeasurement, RoadPath> timeStep) {
        for (RoadPosition candidate : timeStep.candidates) {
            final double distance =
                computeDistance(candidate.position, timeStep.observation.position);
            timeStep.addEmissionLogProbability(candidate,
                hmmProbabilities.emissionLogProbability(distance));
        }
    }

    private void computeTransitionProbabilities(
        TimeStep<RoadPosition, GpsMeasurement, RoadPath> prevTimeStep,
        TimeStep<RoadPosition, GpsMeasurement, RoadPath> timeStep) {
        final double linearDistance = computeDistance(prevTimeStep.observation.position,
            timeStep.observation.position);
        final double timeDiff = (timeStep.observation.time.getTime() -
            prevTimeStep.observation.time.getTime()) / 1000.0;

        for (RoadPosition from : prevTimeStep.candidates) {
            for (RoadPosition to : timeStep.candidates) {

                // For real map matching applications, route lengths and road paths would be
                // computed using a router. The most efficient way is to use a single-source
                // multi-target router.
                final double routeLength = routeLengths.get(new Transition<>(from, to));
                timeStep.addRoadPath(from, to, new RoadPath(from, to));

                final double transitionLogProbability = hmmProbabilities.transitionLogProbability(
                    routeLength, linearDistance, timeDiff);
                timeStep.addTransitionLogProbability(from, to, transitionLogProbability);
            }
        }
    }


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        Integer trajId = input.getIntegerByField("trajId");
        GpsMeasurement gps =
            new GpsMeasurement(seconds(input.getIntegerByField("timestamp")), input.getDoubleByField("lat"),
                input.getDoubleByField("lng"));
        LOG.info(trajId + " " + gps.toString());

        final Collection<RoadPosition> candidates = computeCandidates(input.getIntegerByField("timestamp"));
        final TimeStep<RoadPosition, GpsMeasurement, RoadPath> timeStep =
            new TimeStep<>(gps, candidates);
        computeEmissionProbabilities(timeStep);
        if (prevTimeStep == null) {
            viterbi.startWithInitialObservation(timeStep.observation, timeStep.candidates,
                timeStep.emissionLogProbabilities);
        } else {
            computeTransitionProbabilities(prevTimeStep, timeStep);
            viterbi.nextStep(timeStep.observation, timeStep.candidates,
                timeStep.emissionLogProbabilities, timeStep.transitionLogProbabilities,
                timeStep.roadPaths);
        }
        if (prevTimeStep == null) {
            LOG.info("prevTimeStep is null");
        } else {
            LOG.info("prevTimeStep is not null");
        }
        prevTimeStep = timeStep;
        SequenceState<RoadPosition, GpsMeasurement, RoadPath> roadPosition = viterbi.computeMostLikelySequence().get(0);
        // FIXME: dist
        collector.emit(new Values(trajId, roadPosition.state.edgeId, 0.0));
        LOG.info("success to emit");
    }
}

