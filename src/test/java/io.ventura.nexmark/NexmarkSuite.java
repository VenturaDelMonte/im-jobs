package io.ventura.nexmark;

import io.ventura.nexmark.NexmarkQuery5.NexmarkQuery5;
import io.ventura.nexmark.NexmarkQuery8.NexmarkQuery8;
import io.ventura.nexmark.NexmarkQueryX.NexmarkQueryX;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReplicationOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class NexmarkSuite {

	private static final Logger LOG = LoggerFactory.getLogger(NexmarkSuite.class);


	private static final int numTaskManagers = 8;
	private static final int slotsPerTaskManager = 1;


	private static TestingCluster cluster;

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@BeforeClass
	public static void setup() throws Exception {
		// detect parameter change

		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers);
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);
		config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, 16 * 1024 * 1024);
		config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 32 * 1024 * 1024);
		config.setFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION, 0.2f);

		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		config.setInteger(WebOptions.PORT, 8081);

		config.setString(ReplicationOptions.TASK_MANAGER_CHECKPOINT_READER, "zero-copy-fadvise-lazy");
		config.setInteger(ReplicationOptions.BIN_PACKING_SERVER_CACHE, 2);
		config.setString(CheckpointingOptions.STATE_BACKEND, "custom");
		config.setBoolean(NettyConfig.REPLICATION_SHARE_NETTY_THREADS_POOL, false);
		config.setInteger(NettyConfig.REPLICATION_NUM_OF_ARENAS, 2);
		config.setInteger(ReplicationOptions.NETWORK_BUFFERS_SIZE, 32768/2);
		config.setInteger(NettyConfig.STATE_REPLICATION_LOW_WATERMARK_FACTOR, 1);
		config.setInteger(NettyConfig.STATE_REPLICATION_HIGH_WATERMARK_FACTOR, 2);
		config.setInteger(ReplicationOptions.STATE_REPLICATION_NUM_CONNECTIONS_POOL, 1);
		config.setInteger(ReplicationOptions.NETWORK_BUFFERS_PER_REPLICA, 4);
		config.setInteger(ReplicationOptions.NETWORK_BUFFERS_PER_REPLICA_MAX, 8);
		config.setBoolean(ReplicationOptions.TASK_MANAGER_STATE_ROCKSDB_LOGGING, false);
		config.setString(ReplicationOptions.TASK_MANAGER_STATE_ROCKSDB_PREDEF, "ssd");
		config.setInteger(ReplicationOptions.STATE_REPLICATION_REPLICA_SLOTS, 220);
		config.setInteger("state.replication.ack.timeout", 3 * 60 * 1000);
		config.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, false);
		config.setInteger(ReplicationOptions.STATE_REPLICATION_FACTOR, 1);
		config.setString(ReplicationOptions.JM_REPLICATION_DIRECTORY, System.getProperty("java.io.tmpdir") + "/jm");
		config.setString(ReplicationOptions.REPLICATION_DIRECTORY, System.getProperty("java.io.tmpdir") + "/t1:" + System.getProperty("java.io.tmpdir") + "/t2");
		config.setInteger(NettyConfig.REPLICATION_NUM_THREADS_SERVER, 2);
		config.setInteger(NettyConfig.REPLICATION_NUM_THREADS_CLIENT, 2);
		config.setLong(ReplicationOptions.NETWORK_REPLICA_BUFFERS_MEMORY_MIN, 100 * 1024 * 1024);
		config.setLong(ReplicationOptions.NETWORK_REPLICA_BUFFERS_MEMORY_MAX, 1024 * 1024 * 1024);
		config.setFloat(ReplicationOptions.NETWORK_REPLICA_BUFFERS_MEMORY_FRACTION, 0.1f);


		cluster = new TestingCluster(config);
		cluster.start();

	}

	@AfterClass
	public static void shutDownExistingCluster() {
		if (cluster != null) {
			cluster.stop();
		}
	}

	@Test
	public void runNexmarkDebug() throws Exception {

		Map<String, String> config = new HashMap<>();

		config.put("autogen", "1");
		config.put("personsInputSizeGb", "1");
		config.put("desiredPersonsThroughputMb", "100");
		//config.put("checkpointingInterval", "5000");

		ParameterTool params = ParameterTool.fromMap(config);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		NexmarkQuery8.runNexmarkQ8Debug(env, params);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		FiniteDuration timeout = new FiniteDuration(10, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		cluster.getLeaderGateway(deadline.timeLeft());

		cluster.submitJobAndWait(jobGraph, true);
	}

	@Test
	public void runNexmarkQ8() throws Exception {

		Map<String, String> config = new HashMap<>();

		config.put("windowDuration", "500000");
		config.put("checkpointingInterval", "60000");
//		config.put("checkpointingTimeout", ""+(2*60*1000));
		config.put("windowParallelism", "4");
		config.put("sourceParallelism", "2");
		config.put("minPauseBetweenCheckpoints", "10000");
		config.put("sinkParallelism", "4");
		config.put("autogen", "1");

		ParameterTool params = ParameterTool.fromMap(config);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		NexmarkQuery8.runNexmarkQ8(env, params);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		FiniteDuration timeout = new FiniteDuration(10, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		ActorGateway jobManager = cluster.getLeaderGateway(deadline.timeLeft());

		cluster.submitJobAndWait(jobGraph, true);
	}

	@Test
	public void runNexmarkQ5() throws Exception {

		Map<String, String> config = new HashMap<>();

		config.put("checkpointingInterval", "60000");
//		config.put("checkpointingTimeout", ""+(2*60*1000));
		config.put("windowParallelism", "4");
		config.put("numOfVirtualNodes", "1");
		config.put("sourceParallelism", "2");
		config.put("minPauseBetweenCheckpoints", "10000");
		config.put("sinkParallelism", "4");
		config.put("autogen", "1");

		ParameterTool params = ParameterTool.fromMap(config);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		NexmarkQuery5.runNexmarkQ5(env, params);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		FiniteDuration timeout = new FiniteDuration(10, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		ActorGateway jobManager = cluster.getLeaderGateway(deadline.timeLeft());

		cluster.submitJobAndWait(jobGraph, true);
	}

	@Test
	public void runNexmarkQX() throws Exception {

		Map<String, String> config = new HashMap<>();

		config.put("checkpointingInterval", "60000");
//		config.put("checkpointingTimeout", ""+(2*60*1000));
		config.put("windowParallelism", "4");
		config.put("numOfVirtualNodes", "1");
		config.put("sourceParallelism", "2");
		config.put("minPauseBetweenCheckpoints", "10000");
		config.put("sinkParallelism", "4");
		config.put("autogen", "1");

		ParameterTool params = ParameterTool.fromMap(config);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		NexmarkQueryX.runNexmarkQX(env, params);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		FiniteDuration timeout = new FiniteDuration(10, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		ActorGateway jobManager = cluster.getLeaderGateway(deadline.timeLeft());

		cluster.submitJobAndWait(jobGraph, true);
	}

	@Test
	public void runNexmarkQXWithHandover() throws Exception {

		Map<String, String> config = new HashMap<>();

		config.put("checkpointingInterval", "5000");
//		config.put("checkpointingTimeout", ""+(2*60*1000));
		config.put("windowParallelism", "4");
		config.put("numOfVirtualNodes", "1");
		config.put("sourceParallelism", "2");
		config.put("minPauseBetweenCheckpoints", "10000");
		config.put("sinkParallelism", "4");
		config.put("autogen", "1");

		ParameterTool params = ParameterTool.fromMap(config);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		NexmarkQueryX.runNexmarkQX(env, params);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		FiniteDuration timeout = new FiniteDuration(10, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		ActorGateway jobManager = cluster.getLeaderGateway(deadline.timeLeft());

		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(5000);
					Future<Object> response = jobManager.ask(new JobManagerMessages.RemoveTaskManager(new ResourceID(0, 0).getResourceIdString()), timeout);
					CompletableFuture<Object> responseFuture = FutureUtils.<Object>toJava(response);
					Object o = responseFuture.get();
					LOG.debug("{}", o);
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
		}).start();

				cluster.submitJobAndWait(jobGraph, true);
	}
}
