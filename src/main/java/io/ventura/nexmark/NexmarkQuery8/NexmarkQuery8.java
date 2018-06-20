package io.ventura.nexmark.NexmarkQuery8;

import io.ventura.nexmark.beans.AuctionEvent0;
import io.ventura.nexmark.beans.NewPersonEvent0;
import io.ventura.nexmark.beans.Query8WindowOutput;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class NexmarkQuery8 {

	private static final Logger LOG = LoggerFactory.getLogger(NexmarkQuery8.class);

	private static final int[] PORTS = {31337, 31001, 31002, 31003, 31004, 31005, 31006, 31007, 31008, 31009};

	private static final String PERSONS_TOPIC = "nexmark_persons";
	private static final String AUCTIONS_TOPIC = "nexmark_auctions";

	static boolean IsCloudMachineNumber(String string) {
		try {
			int num = Integer.parseInt(string);
			return num >= 7 && num <= 37;
		} catch (Exception e) {
			return false;
		}
	}

	static boolean IsSingleDigit(String string) {
		return (Integer.valueOf(string) / 10) == 0;
	}

	public static List<Integer> parsePorts(String ports_string) {
		List<String> separated_ports = Arrays.asList(ports_string.split(","));

		List<Integer> ports = new ArrayList<>();
		for (String port : separated_ports) {
			if (IsSingleDigit(port)) {
				ports.add(PORTS[Integer.valueOf(port)]);
			} else {
				ports.add(Integer.valueOf(port));
			}
		}

		return ports;
	}

	public static List<String> parseHostnames(String hostnames_string) {
		List<String> hostnames = Arrays.asList(hostnames_string.split(","));

		for (int i = 0; i < hostnames.size(); i++) {
			if (IsCloudMachineNumber(hostnames.get(i))) {
				hostnames.set(i, "cloud-" + hostnames.get(i));
			} else if (hostnames.get(i).equals("l")) {
				hostnames.set(i, "localhost");
			}
		}

		return hostnames;
	}

	private static class PersonDeserializationSchema implements KeyedDeserializationSchema<NewPersonEvent0[]> {

		private static final TypeInformation<NewPersonEvent0[]> FLINK_INTERNAL_TYPE = TypeInformation.of(new TypeHint<NewPersonEvent0[]>() {});

		@Override
		public NewPersonEvent0[] deserialize(
				byte[] messageKey,
				byte[] buffer,
				String topic,
				int partition,
				long offset) throws IOException {

			ByteBuffer wrapper = ByteBuffer.wrap(buffer);
			int checksum = wrapper.getInt();
			int itemsInThisBuffer = wrapper.getInt();

			Preconditions.checkArgument(checksum == 0x30011991);

			NewPersonEvent0[] data = new NewPersonEvent0[itemsInThisBuffer];

			byte[] tmp = new byte[4 * 10];

			StringBuilder helper = new StringBuilder(64);
			for (int i = 0; i < data.length; i++) {
				long id = wrapper.getLong();
				wrapper.get(tmp);
				long c = wrapper.getLong();
				int a = wrapper.getInt();
				int b = wrapper.getInt();
				long ts = wrapper.getLong();
				String s = new String(tmp);
				String cc = helper.append(c).append(a).append(b).toString();
				data[i] = new NewPersonEvent0(ts, id, s, s, s, s, s, cc, cc, cc);
				helper.setLength(0);
			}

			return data;
		}

		@Override
		public boolean isEndOfStream(NewPersonEvent0[] nextElement) {
			return false;
		}

		@Override
		public TypeInformation<NewPersonEvent0[]> getProducedType() {
			return FLINK_INTERNAL_TYPE;
		}
	}

	private static class AuctionsDeserializationSchema implements KeyedDeserializationSchema<AuctionEvent0[]> {

		private static final TypeInformation<AuctionEvent0[]> FLINK_INTERNAL_TYPE = TypeInformation.of(new TypeHint<AuctionEvent0[]>() {});

		@Override
		public AuctionEvent0[] deserialize(
				byte[] messageKey,
				byte[] buffer,
				String topic,
				int partition,
				long offset) throws IOException {

			ByteBuffer wrapper = ByteBuffer.wrap(buffer);
			int checksum = wrapper.getInt();
			int itemsInThisBuffer = wrapper.getInt();

			Preconditions.checkArgument(checksum == 0x30061992);

			AuctionEvent0[] data = new AuctionEvent0[itemsInThisBuffer];

			for (int i = 0; i < data.length; i++) {
				long id = wrapper.getLong();
				long pid = wrapper.getLong();
				byte c = wrapper.get();
				int itemId = wrapper.getInt();
				long start = wrapper.getLong();
				long end = wrapper.getLong();
				int price = wrapper.getInt();
				long ts = wrapper.getLong();
				data[i] = new AuctionEvent0(ts, id, itemId, pid, (double) price, c, start, end);
			}

			return data;
		}

		@Override
		public boolean isEndOfStream(AuctionEvent0[] nextElement) {
			return false;
		}

		@Override
		public TypeInformation<AuctionEvent0[]> getProducedType() {
			return FLINK_INTERNAL_TYPE;
		}
	}

	public static class JoiningNewUsersWithAuctionsCoGroupFunction extends RichCoGroupFunction<NewPersonEvent0, AuctionEvent0, Query8WindowOutput> {

		private static final Logger LOG = LoggerFactory.getLogger(JoiningNewUsersWithAuctionsCoGroupFunction.class);

		/**
		 * CoGroups Auction and Person on person id and return the Persons name as well as ID.
		 * Finding every person that created a new auction.
		 *
		 * Currently, when execution on the simple generator, it most certainly will happen, that the same person
		 * appears multiple times in a window. Currently, simple ignore that case.
		 */
		@Override
		public void coGroup(
				Iterable<NewPersonEvent0> persons,
				Iterable<AuctionEvent0> auctions,
				Collector<Query8WindowOutput> out) {

			Iterator<NewPersonEvent0> personIterator = persons.iterator();
			Iterator<AuctionEvent0> auctionIterator = auctions.iterator();

			if (!auctionIterator.hasNext()) {
				return;
			}

			while (personIterator.hasNext()) {
				NewPersonEvent0 person = personIterator.next();

				long ts = System.currentTimeMillis();
				long auctionCreationTimestampLatest = Long.MIN_VALUE;
				long auctionIngestionTimestampLatest = Long.MIN_VALUE;
				for (AuctionEvent0 auction : auctions) {
					long auctionIngestionTimestamp = auction.getIngestionTimestamp();
					if (auctionIngestionTimestamp > auctionIngestionTimestampLatest) {
						auctionIngestionTimestampLatest = auctionIngestionTimestamp;
						auctionCreationTimestampLatest = auction.getTimestamp();
					}
				}
				out.collect(new Query8WindowOutput(
							ts,
							person.getTimestamp(),
							person.getIngestionTimestamp(),
							auctionCreationTimestampLatest,
							auctionIngestionTimestampLatest,
							person.getPersonId()));
			}
		}
	}

	private static final class PersonsFlatMapper implements FlatMapFunction<NewPersonEvent0[], NewPersonEvent0> {
		@Override
		public void flatMap(NewPersonEvent0[] items, Collector<NewPersonEvent0> out) throws Exception {
			for (NewPersonEvent0 item : items) {
				out.collect(item);
			}
		}
	}

	private static final class AuctionsFlatMapper implements FlatMapFunction<AuctionEvent0[], AuctionEvent0> {
		@Override
		public void flatMap(AuctionEvent0[] items, Collector<AuctionEvent0> out) throws Exception {
			for (AuctionEvent0 item : items) {
				out.collect(item);
			}
		}
	}

	private static final class NexmarkQuery8LatencyTrackingSink extends RichSinkFunction<Query8WindowOutput> {

		private transient StringBuilder buffer;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			buffer = new StringBuilder(256);
		}

		@Override
		public void invoke(Query8WindowOutput record, Context context) throws Exception {
			long timeMillis = context.currentProcessingTime();
			try {
				buffer.append(timeMillis);
				buffer.append(",");
				buffer.append(timeMillis - record.getWindowEvictingTimestamp());
				buffer.append(",");
				buffer.append(timeMillis - record.getAuctionCreationTimestamp());
				buffer.append(",");
				buffer.append(timeMillis - record.getPersonCreationTimestamp());
				buffer.append(",");
				buffer.append(record.getPersonId());
				LOG.info("Nexmark8Sink - {}", buffer.toString());
			} finally {
				buffer.setLength(0);
			}
		}
	}

	public static void runNexmark(StreamExecutionEnvironment env, ParameterTool params) throws Exception {

		final int sourceParallelism = params.getInt("sourceParallelism", 1);
		final int windowParallelism = params.getInt("windowParallelism", 1);
		final int windowDuration = params.getInt("windowDuration", 1);
		final int sinkParallelism = params.getInt("sinkParallelism", windowDuration);

		final int checkpointingInterval = params.getInt("checkpointingInterval", 0);
		final long checkpointingTimeout = params.getLong("checkpointingTimeout", CheckpointConfig.DEFAULT_TIMEOUT);
		final int concurrentCheckpoints = params.getInt("concurrentCheckpoints", 1);
		final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
		final int minPauseBetweenCheckpoints = params.getInt("minPauseBetweenCheckpoints", checkpointingInterval);
		final int parallelism = params.getInt("parallelism", 1);
		final int maxParallelism = params.getInt("maxParallelism", 1024);
		final int numOfVirtualNodes = params.getInt("numOfVirtualNodes", 4);

		final String kafkaServers = params.get("kafkaServers", "localhost:9092");

		Properties baseCfg = new Properties();

		baseCfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setRestartStrategy(RestartStrategies.noRestart());
		if (checkpointingInterval > 0) {
			env.enableCheckpointing(checkpointingInterval);
			env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
			env.getCheckpointConfig().setMaxConcurrentCheckpoints(concurrentCheckpoints);
			env.getCheckpointConfig().setCheckpointTimeout(checkpointingTimeout);
			env.getCheckpointConfig().setFailOnCheckpointingErrors(true);
		}
		env.setParallelism(parallelism);
		env.setMaxParallelism(maxParallelism);
		env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);

		env.getConfig().registerTypeWithKryoSerializer(AuctionEvent0.class, AuctionEvent0.AuctionEventKryoSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(NewPersonEvent0.class, NewPersonEvent0.NewPersonEventKryoSerializer.class);

		FlinkKafkaConsumer011<NewPersonEvent0[]> kafkaSourcePersons =
				new FlinkKafkaConsumer011<>(PERSONS_TOPIC, new PersonDeserializationSchema(), baseCfg);

		FlinkKafkaConsumer011<AuctionEvent0[]> kafkaSourceAuctions =
				new FlinkKafkaConsumer011<>(AUCTIONS_TOPIC, new AuctionsDeserializationSchema(), baseCfg);

		kafkaSourceAuctions.setCommitOffsetsOnCheckpoints(true);
		kafkaSourceAuctions.setStartFromEarliest();
		kafkaSourcePersons.setCommitOffsetsOnCheckpoints(true);
		kafkaSourcePersons.setStartFromEarliest();

		DataStream<NewPersonEvent0> in1 = env
				.addSource(kafkaSourcePersons)
				.name("NewPersonsInputStream").setParallelism(sourceParallelism)
				.flatMap(new PersonsFlatMapper())
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NewPersonEvent0>(Time.seconds(1)) {

					@Override
					public long extractTimestamp(NewPersonEvent0 newPersonEvent) {
						return newPersonEvent.timestamp;
					}
			})
		;

		DataStream<AuctionEvent0> in2 = env
				.addSource(kafkaSourceAuctions)
				.name("AuctionEventInputStream").setParallelism(sourceParallelism)
				.flatMap(new AuctionsFlatMapper())
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AuctionEvent0>(Time.seconds(1)) {
					@Override
					public long extractTimestamp(AuctionEvent0 auctionEvent) {
						return auctionEvent.timestamp;
					}
				})
		;


		in1
			.coGroup(in2)
				.where(NewPersonEvent0::getPersonId)
				.equalTo(AuctionEvent0::getPersonId)
				.window(TumblingEventTimeWindows.of(Time.seconds(windowDuration)))
				.with(new JoiningNewUsersWithAuctionsCoGroupFunction())
				.name("WindowOperator")
				.setParallelism(windowParallelism)
				.setVirtualNodesNum(numOfVirtualNodes)
			.addSink(new NexmarkQuery8LatencyTrackingSink())
				.name("Nexmark8Sink")
				.setParallelism(sinkParallelism);


	}

	public static void runNexmarkDebug(StreamExecutionEnvironment env, ParameterTool params) throws Exception {

		final int sourceParallelism = params.getInt("sourceParallelism", 1);
		final int checkpointingInterval = params.getInt("checkpointingInterval", 0);
		final long checkpointingTimeout = params.getLong("checkpointingTimeout", CheckpointConfig.DEFAULT_TIMEOUT);
		final int concurrentCheckpoints = params.getInt("concurrentCheckpoints", 1);
		final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
		final int minPauseBetweenCheckpoints = params.getInt("minPauseBetweenCheckpoints", checkpointingInterval);
		final int parallelism = params.getInt("parallelism", 1);
		final int maxParallelism = params.getInt("maxParallelism", 1024);
		final int numOfVirtualNodes = params.getInt("numOfVirtualNodes", 4);
		final String kafkaServers = params.get("kafkaServers", "localhost:9092");


		Properties baseCfg = new Properties();

		baseCfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setRestartStrategy(RestartStrategies.noRestart());
		if (checkpointingInterval > 0) {
			env.enableCheckpointing(checkpointingInterval);
			env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
			env.getCheckpointConfig().setMaxConcurrentCheckpoints(concurrentCheckpoints);
			env.getCheckpointConfig().setCheckpointTimeout(checkpointingTimeout);
		}
		env.setParallelism(parallelism);
		env.setMaxParallelism(maxParallelism);
		env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);

		env.getConfig().registerTypeWithKryoSerializer(AuctionEvent0.class, AuctionEvent0.AuctionEventKryoSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(NewPersonEvent0.class, NewPersonEvent0.NewPersonEventKryoSerializer.class);

		FlinkKafkaConsumer011<NewPersonEvent0[]> kafkaSource =
				new FlinkKafkaConsumer011<>(PERSONS_TOPIC, new PersonDeserializationSchema(), baseCfg);

		kafkaSource.setStartFromEarliest();
		kafkaSource.setCommitOffsetsOnCheckpoints(true);

		env
				.addSource(kafkaSource)
				.name("NewPersonsInputStream").setParallelism(sourceParallelism)
				.flatMap(new FlatMapFunction<NewPersonEvent0[], NewPersonEvent0>() {
					@Override
					public void flatMap(NewPersonEvent0[] items, Collector<NewPersonEvent0> out) throws Exception {
						for (NewPersonEvent0 item : items) {
							out.collect(item);
						}
					}
				})
//				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NewPersonEvent0>(Time.seconds(1)) {
//					@Override
//					public long extractTimestamp(NewPersonEvent0 newPersonEvent) {
//						return newPersonEvent.getTimestamp();
//					}
//				})
				.addSink(new SinkFunction<NewPersonEvent0>() {
					@Override
					public void invoke(NewPersonEvent0 value, Context context) throws Exception {

					}
				}).setParallelism(sourceParallelism);



	}

}