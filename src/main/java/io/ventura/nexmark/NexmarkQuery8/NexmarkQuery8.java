package io.ventura.nexmark.NexmarkQuery8;

import io.ventura.nexmark.beans.AuctionEvent0;
import io.ventura.nexmark.beans.NewPersonEvent0;
import io.ventura.nexmark.beans.Query8WindowOutput;
import io.ventura.nexmark.original.Cities;
import io.ventura.nexmark.original.Countries;
import io.ventura.nexmark.original.Emails;
import io.ventura.nexmark.original.Firstnames;
import io.ventura.nexmark.original.Lastnames;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.DynamicEventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class NexmarkQuery8 {

	private static final Logger LOG = LoggerFactory.getLogger(NexmarkQuery8.class);

	private static final long ONE_GIGABYTE = 1024L * 1024L * 1024L;

	private static final String PERSONS_TOPIC = "nexmark_persons";
	private static final String AUCTIONS_TOPIC = "nexmark_auctions";

	private static class PersonDeserializationSchema implements KeyedDeserializationSchema<NewPersonEvent0[]> {

		private static final int PERSON_RECORD_SIZE = 206;

		private static final TypeInformation<NewPersonEvent0[]> FLINK_INTERNAL_TYPE = TypeInformation.of(new TypeHint<NewPersonEvent0[]>() {});

		//private final long bytesToRead;

//		private long bytesReadSoFar;

		private long lastBacklog = Long.MAX_VALUE;

		public PersonDeserializationSchema() {
			//this.bytesToRead = (bytesToRead / PERSON_RECORD_SIZE) * PERSON_RECORD_SIZE;
//			this.bytesReadSoFar = 0;
		}

		@Override
		public NewPersonEvent0[] deserialize(
				byte[] messageKey,
				byte[] buffer,
				String topic,
				int partition,
				long offset) throws IOException {

			Preconditions.checkArgument(buffer.length == 8192);
			ByteBuffer wrapper = ByteBuffer.wrap(buffer);
			int checksum = wrapper.getInt();
			int itemsInThisBuffer = wrapper.getInt();
			long newBacklog = wrapper.getLong();

			Preconditions.checkArgument(((8192 - 16) / PERSON_RECORD_SIZE) >= itemsInThisBuffer); 

			Preconditions.checkArgument(checksum == 0x30011991);

			NewPersonEvent0[] data = new NewPersonEvent0[itemsInThisBuffer];

			byte[] tmp = new byte[32];

			long ingestionTimestamp = System.currentTimeMillis();

			for (int i = 0; i < data.length; i++) {
				long id = wrapper.getLong();
				wrapper.get(tmp);
				String name = new String(Arrays.copyOf(tmp, tmp.length));
				wrapper.get(tmp);
				String surname = new String(Arrays.copyOf(tmp, tmp.length));
				wrapper.get(tmp);
				String email = name + "." + surname + "@" + new String(Arrays.copyOf(tmp, tmp.length));
				wrapper.get(tmp);
				String city = new String(Arrays.copyOf(tmp, tmp.length));
				wrapper.get(tmp);
				String country = new String(Arrays.copyOf(tmp, tmp.length));
				long creditCard0 = wrapper.getLong();
				long creditCard1 = wrapper.getLong();
				int a = wrapper.getInt();
				int b = wrapper.getInt();
				int c = wrapper.getInt();
				short maleOrFemale = wrapper.getShort();
				long timestamp = wrapper.getLong(); // 128
//				Preconditions.checkArgument(timestamp > 0);
				data[i] = new NewPersonEvent0(
						timestamp,
						id,
						name + " " + surname,
						email,
						city,
						country,
						"" + (a - c),
						"" + (b - c),
						email,
						"" + (creditCard0 + creditCard1),
						ingestionTimestamp);
			}

//			bytesReadSoFar += buffer.length;
			Preconditions.checkArgument(newBacklog < lastBacklog, "newBacklog: %s oldBacklog: %s", newBacklog, lastBacklog);
			lastBacklog = newBacklog;

			return data;
		}

		@Override
		public boolean isEndOfStream(NewPersonEvent0[] nextElement) {
			return lastBacklog <= 0;
		}

		@Override
		public TypeInformation<NewPersonEvent0[]> getProducedType() {
			return FLINK_INTERNAL_TYPE;
		}
	}

	private static class AuctionsDeserializationSchema implements KeyedDeserializationSchema<AuctionEvent0[]> {

		private static final int AUCTION_RECORD_SIZE = 269;

		private static final TypeInformation<AuctionEvent0[]> FLINK_INTERNAL_TYPE = TypeInformation.of(new TypeHint<AuctionEvent0[]>() {});

//		private final long bytesToRead;

//		private long bytesReadSoFar;

		private long lastBacklog = Long.MAX_VALUE;

		public AuctionsDeserializationSchema() {
//			this.bytesToRead = (bytesToRead / AUCTION_RECORD_SIZE) * AUCTION_RECORD_SIZE;
//			this.bytesReadSoFar = 0;
		}

		@Override
		public AuctionEvent0[] deserialize(
				byte[] messageKey,
				byte[] buffer,
				String topic,
				int partition,
				long offset) throws IOException {

			Preconditions.checkArgument(buffer.length == 8192);

			ByteBuffer wrapper = ByteBuffer.wrap(buffer);
			int checksum = wrapper.getInt();
			int itemsInThisBuffer = wrapper.getInt();
			long newBacklog = wrapper.getLong();

			Preconditions.checkArgument(checksum == 0x30061992);
			Preconditions.checkArgument(((8192 - 16) / AUCTION_RECORD_SIZE) >= itemsInThisBuffer);

			AuctionEvent0[] data = new AuctionEvent0[itemsInThisBuffer];
			long ingestionTimestamp = System.currentTimeMillis();

			byte[] tmp0 = new byte[20];
			byte[] tmp1 = new byte[200];

			for (int i = 0; i < data.length; i++) {
				long id = wrapper.getLong();
				long pid = wrapper.getLong();
				byte c = wrapper.get();
				int itemId = wrapper.getInt();
				long start = wrapper.getLong();
				long end = wrapper.getLong();
				int price = wrapper.getInt();
				wrapper.get(tmp0);
				wrapper.get(tmp1);
				long ts = wrapper.getLong();
//				Preconditions.checkArgument(ts > 0);
				data[i] = new AuctionEvent0(
						ts,
						id,
						new String(Arrays.copyOf(tmp0, tmp0.length)),
						new String(Arrays.copyOf(tmp1, tmp1.length)),
						itemId,
						pid,
						(double) price,
						c,
						start,
						end,
						ingestionTimestamp);
			}

//			bytesReadSoFar += buffer.length;
			Preconditions.checkArgument(newBacklog < lastBacklog, "newBacklog: %s oldBacklog: %s", newBacklog, lastBacklog);
			lastBacklog = newBacklog;

			return data;
		}

		@Override
		public boolean isEndOfStream(AuctionEvent0[] nextElement) {
			return lastBacklog <= 0;
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
			for (int i = 0; i < items.length; i++) {
				out.collect(items[i]);
			}
		}
	}

	private static final class AuctionsFlatMapper implements FlatMapFunction<AuctionEvent0[], AuctionEvent0> {
		@Override
		public void flatMap(AuctionEvent0[] items, Collector<AuctionEvent0> out) throws Exception {
			for (int i = 0; i < items.length; i++) {
				out.collect(items[i]);
			}
		}
	}

	private static final class SinkLatencyTrackingHistogramStatistics extends HistogramStatistics {

		private final SummaryStatistics impl;

		public SinkLatencyTrackingHistogramStatistics(SummaryStatistics original) {
			this.impl = original.copy();
		}

		@Override
		public double getQuantile(double v) {
			return -1;
		}

		@Override
		public long[] getValues() {
			return new long[] {-1};
		}

		@Override
		public int size() {
			return (int) impl.getN();
		}

		@Override
		public double getMean() {
			return impl.getMean();
		}

		@Override
		public double getStdDev() {
			return impl.getStandardDeviation();
		}

		@Override
		public long getMax() {
			return (long) impl.getMax();
		}

		@Override
		public long getMin() {
			return (long) impl.getMin();
		}
	}

	private static final class SinkLatencyTrackingHistogram implements Histogram {

		private final SummaryStatistics impl = new SummaryStatistics();

		@Override
		public void update(long l) {
			impl.addValue(l);
		}

		@Override
		public long getCount() {
			return impl.getN();
		}

		@Override
		public HistogramStatistics getStatistics() {
			return new SinkLatencyTrackingHistogramStatistics(impl);
		}
	}

	private static final class NexmarkQuery8LatencyTrackingSink extends RichSinkFunction<Query8WindowOutput> {

//		private transient StringBuilder buffer;
		private transient Histogram sinkLatencyWindowEviction;
		private transient Histogram sinkLatencyPersonCreation;
		private transient Histogram sinkLatencyAuctionCreation;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
//			buffer = new StringBuilder(256);
			sinkLatencyWindowEviction = getRuntimeContext().getMetricGroup().histogram("sinkLatencyWindowEviction", new SinkLatencyTrackingHistogram());
			sinkLatencyPersonCreation = getRuntimeContext().getMetricGroup().histogram("sinkLatencyPersonCreation", new SinkLatencyTrackingHistogram());
			sinkLatencyAuctionCreation = getRuntimeContext().getMetricGroup().histogram("sinkLatencyAuctionCreation", new SinkLatencyTrackingHistogram());
		}

		@Override
		public void invoke(Query8WindowOutput record, Context context) throws Exception {
			long timeMillis = context.currentProcessingTime();
			sinkLatencyPersonCreation.update(timeMillis - record.getPersonCreationTimestamp());
			sinkLatencyWindowEviction.update(timeMillis - record.getWindowEvictingTimestamp());
			sinkLatencyAuctionCreation.update(timeMillis - record.getAuctionCreationTimestamp());
//			try {
//				buffer.append(timeMillis);
//				buffer.append(",");
//				buffer.append(timeMillis - record.getWindowEvictingTimestamp());
//				buffer.append(",");
//				buffer.append(timeMillis - record.getAuctionCreationTimestamp());
//				buffer.append(",");
//				buffer.append(timeMillis - record.getPersonCreationTimestamp());
//				buffer.append(",");
//				buffer.append(record.getPersonId());
//				LOG.info("Nexmark8Sink - {}", buffer.toString());
//			} finally {
//				buffer.setLength(0);
//			}
		}
	}


	private static class PersonAutoGen extends RichParallelSourceFunction<NewPersonEvent0> {

		private volatile boolean keepGoing = true;

		private static final AtomicLong currentPersonId = new AtomicLong();

		@Override
		public void run(SourceContext<NewPersonEvent0> ctx) throws Exception {
			ThreadLocalRandom r = ThreadLocalRandom.current();
			Thread.sleep(r.nextInt(100, 500));
			while (keepGoing) {
				synchronized (ctx.getCheckpointLock()) {
					for (int i = 0; i < 32; i++) {
						int ifn = r.nextInt(Firstnames.NUM_FIRSTNAMES);
						int iln = r.nextInt(Lastnames.NUM_LASTNAMES);
						int iem = r.nextInt(Emails.NUM_EMAILS);
						int ict = r.nextInt(Countries.NUM_COUNTRIES);
						int icy = r.nextInt(Cities.NUM_CITIES);
						long now = System.currentTimeMillis();
						ctx.collect(new NewPersonEvent0(
								now,
								currentPersonId.getAndIncrement(),
								Firstnames.FIRSTNAMES_32[ifn] + " " + Lastnames.LASTNAMES_32[iln],
								new String(Emails.EMAILS_32[iem]),
								new String(Cities.CITIES_32[icy]),
								new String(Countries.COUNTRIES_32[ict]),
								"123456789012312312",
								"123456789012312312",
								new String(Emails.EMAILS_32[iem]),
								"123456789012312312",
								now));
					}
				}
				Thread.sleep(200);
			}
			ctx.close();
		}

		@Override
		public void cancel() {
			keepGoing = false;
		}
	}

	public static class AuctionAutoGen extends RichParallelSourceFunction<AuctionEvent0> {

		private volatile boolean keepGoing = true;

		private static final AtomicLong currentAuctionId = new AtomicLong();


		@Override
		public void run(SourceContext<AuctionEvent0> ctx) throws Exception {
			ThreadLocalRandom r = ThreadLocalRandom.current();
			Thread.sleep(r.nextInt(100, 500));
			while (keepGoing) {
				synchronized (ctx.getCheckpointLock()) {
					for (int i = 0; i < 32; i++) {

					}
				}
				Thread.sleep(200);
			}
			ctx.close();
		}

		@Override
		public void cancel() {
			keepGoing = false;
		}
	}

	public static void runNexmark(StreamExecutionEnvironment env, ParameterTool params) throws Exception {

		final int sourceParallelism = params.getInt("sourceParallelism", 1);
		final int windowParallelism = params.getInt("windowParallelism", 1);
		final int windowDuration = params.getInt("windowDuration", 1);
		final int windowType = params.getInt("windowType", 0);
		Preconditions.checkArgument(windowDuration > 0);
		final int windowSlide = params.getInt("windowSlide", windowType == 1 ? windowDuration / 2 : windowDuration);
		final int sinkParallelism = params.getInt("sinkParallelism", windowParallelism);

		final int checkpointingInterval = params.getInt("checkpointingInterval", 0);
		final long checkpointingTimeout = params.getLong("checkpointingTimeout", CheckpointConfig.DEFAULT_TIMEOUT);
		final int concurrentCheckpoints = params.getInt("concurrentCheckpoints", 1);
		final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
		final int minPauseBetweenCheckpoints = params.getInt("minPauseBetweenCheckpoints", checkpointingInterval);
		final int parallelism = params.getInt("parallelism", 1);
		final int maxParallelism = params.getInt("maxParallelism", 1024);
		final int numOfVirtualNodes = params.getInt("numOfVirtualNodes", 4);

		final boolean autogen = params.getBoolean("autogen", false);

		final int numOfReplicaSlotsHint = params.getInt("numOfReplicaSlotsHint", 1);

		final String kafkaServers = params.get("kafkaServers", "localhost:9092");

		Properties baseCfg = new Properties();

		baseCfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		baseCfg.setProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "" + (128 * 1024));
		baseCfg.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "8192");
		baseCfg.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "im-job");
		baseCfg.setProperty("offsets.commit.timeout.ms", "60000");

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

		env.getConfig().enableForceKryo();
		env.getConfig().registerTypeWithKryoSerializer(AuctionEvent0.class, AuctionEvent0.AuctionEventKryoSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(NewPersonEvent0.class, NewPersonEvent0.NewPersonEventKryoSerializer.class);
		env.getConfig().addDefaultKryoSerializer(AuctionEvent0.class, AuctionEvent0.AuctionEventKryoSerializer.class);
		env.getConfig().addDefaultKryoSerializer(NewPersonEvent0.class, NewPersonEvent0.NewPersonEventKryoSerializer.class);
		env.getConfig().registerKryoType(AuctionEvent0.class);
		env.getConfig().registerKryoType(NewPersonEvent0.class);

		DataStream<NewPersonEvent0> in1;
		DataStream<AuctionEvent0> in2;

		if (autogen) {

			in1 = env.addSource(new PersonAutoGen()).name("NewPersonsInputStream").setParallelism(sourceParallelism)
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NewPersonEvent0>(Time.seconds(1)) {
					@Override
					public long extractTimestamp(NewPersonEvent0 newPersonEvent) {
						return newPersonEvent.timestamp;
					}
			}).setParallelism(sourceParallelism).returns(TypeInformation.of(new TypeHint<NewPersonEvent0>() {}));

			in2 = env.addSource(new RichParallelSourceFunction<AuctionEvent0>() {
				@Override
				public void run(SourceContext<AuctionEvent0> ctx) throws Exception {

				}

				@Override
				public void cancel() {

				}
			}).name("AuctionEventInputStream").setParallelism(sourceParallelism)
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AuctionEvent0>(Time.seconds(1)) {
				@Override
				public long extractTimestamp(AuctionEvent0 auctionEvent) {
					return auctionEvent.timestamp;
				}
			}).setParallelism(sourceParallelism).returns(TypeInformation.of(new TypeHint<AuctionEvent0>() {}));


		} else {

			FlinkKafkaConsumer011<NewPersonEvent0[]> kafkaSourcePersons =
				new FlinkKafkaConsumer011<>(PERSONS_TOPIC, new PersonDeserializationSchema(), baseCfg);

			FlinkKafkaConsumer011<AuctionEvent0[]> kafkaSourceAuctions =
					new FlinkKafkaConsumer011<>(AUCTIONS_TOPIC, new AuctionsDeserializationSchema(), baseCfg);

			kafkaSourceAuctions.setCommitOffsetsOnCheckpoints(true);
			kafkaSourceAuctions.setStartFromEarliest();
			kafkaSourcePersons.setCommitOffsetsOnCheckpoints(true);
			kafkaSourcePersons.setStartFromEarliest();

			in1 = env
				.addSource(kafkaSourcePersons)
				.name("NewPersonsInputStream").setParallelism(sourceParallelism)
				.flatMap(new PersonsFlatMapper()).setParallelism(sourceParallelism)
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NewPersonEvent0>(Time.seconds(1)) {
					@Override
					public long extractTimestamp(NewPersonEvent0 newPersonEvent) {
						return newPersonEvent.timestamp;
					}
				}).setParallelism(sourceParallelism).returns(TypeInformation.of(new TypeHint<NewPersonEvent0>() {}))
			;

			in2 = env
				.addSource(kafkaSourceAuctions)
				.name("AuctionEventInputStream").setParallelism(sourceParallelism)
				.flatMap(new AuctionsFlatMapper()).setParallelism(sourceParallelism)
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AuctionEvent0>(Time.seconds(1)) {
					@Override
					public long extractTimestamp(AuctionEvent0 auctionEvent) {
						return auctionEvent.timestamp;
					}
				}).setParallelism(sourceParallelism).returns(TypeInformation.of(new TypeHint<AuctionEvent0>() {}))
			;
		}
		WindowAssigner<Object, TimeWindow> assigner = null;
		switch (windowType) {
			case 0:
				assigner = TumblingEventTimeWindows.of(Time.seconds(windowDuration));
				break;
			case 1:
				assigner = SlidingEventTimeWindows.of(Time.seconds(windowDuration), Time.seconds(windowSlide));
				break;
			case 2:
				assigner = EventTimeSessionWindows.withGap(Time.seconds(windowDuration));
				break;
			default:
				throw new IllegalStateException();
		}

		in1
			.coGroup(in2)
				.where(NewPersonEvent0::getPersonId)
				.equalTo(AuctionEvent0::getPersonId)
				.window(assigner)
				.with(new JoiningNewUsersWithAuctionsCoGroupFunction())
				.name("WindowOperator(" + windowDuration + ")")
				.setParallelism(windowParallelism)
				.setVirtualNodesNum(numOfVirtualNodes)
				.setReplicaSlotsHint(numOfReplicaSlotsHint)
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
		final int personStreamSizeBytes = params.getInt("personStreamSizeBytes", 1);

		Properties baseCfg = new Properties();

		baseCfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		baseCfg.setProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "" + (128 * 1024));

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setRestartStrategy(RestartStrategies.noRestart());
		if (checkpointingInterval > 0) {
			env.enableCheckpointing(checkpointingInterval);
			env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
			env.getCheckpointConfig().setMaxConcurrentCheckpoints(concurrentCheckpoints);
			env.getCheckpointConfig().setCheckpointTimeout(checkpointingTimeout);
		}
		env.getConfig().enableObjectReuse();
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
				}).setParallelism(sourceParallelism)
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

	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ParameterTool params = ParameterTool.fromArgs(args);

		try {
			if (params.getBoolean("debug", false)) {
				runNexmarkDebug(env, params);
			} else {
				runNexmark(env, params);
			}
			env.execute("Nexmark Query 8 (Kafka)");
		} catch (Exception error) {
			LOG.error("Error", error);
		}


	}


}
