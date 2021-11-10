package com.jpro;

import com.jpro.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

@Log4j2
public class KafkaFlinkDemo {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaFlinkDemo.class);

	public static void main(String[] args) {
		log.info("Program KafkaFlinkDemo start ...");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "10.8.7.6:9092");
		properties.setProperty("group.id", "consumer-group");
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("auto.offset.reset", "latest");

		DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties));

		stream.print();
		/** You must use a variable to receive result of current stage! */
		SingleOutputStreamOperator<String> res = stream.map(s -> {
			Json sp;
			try {
				sp = Json.parse(s);
				/** Here, prove that the output of task-manager is stdout! */
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>> stdout test");
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>> stdout test");
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>> stdout test");
				/** Here, prove that the log-system of task-manager is log4j! */
				LOG.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<, log test");
				LOG.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<, log test");
				LOG.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<, log test");
				return new Tuple2<>(sp.getAsText("name"), (Integer) sp.get("number").value());
			} catch (Exception e) {
				return null;
			}
		}).returns(new TypeHint<Tuple2<String, Integer>>() {})
				.filter(Objects::nonNull)
				.keyBy((KeySelector<Tuple2<String, Integer>, Integer>) x -> x.f1)
				/** for testing window mechanism of flink */
				.countWindow(3)
				.reduce((t0, t1) -> new Tuple2<>(t0.f0 + t1.f0, t0.f1))
				/** For convenient observing, we should convert tuple2 to string and send to kafka finally. */
				.map(tp -> new Json("NAME", tp.f0).put("N", tp.f1).dumps());

		res.addSink(new FlinkKafkaProducer("10.8.7.6:9092", "testout", new SimpleStringSchema())).name("toTestOutKafka");
		try {
			env.execute("kafka-test");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
