package org.vitoliu.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 * @author yukun.liu
 * @since 06 十二月 2018
 */
public class SocketStreamWordCount {

	private static final String HOST_NAME="127.0.0.1";
	private static final int PORT = 9000;

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> stream = env.socketTextStream(HOST_NAME, PORT);
		SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplitter()).keyBy(0).sum(1);
		sum.print();
		env.execute("WordCount from socketStream");
	}

	private static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
			String[] tokens = s.toLowerCase().split("\\w+");
			for (String token : tokens) {
				if (StringUtils.isNotEmpty(token)) {
					collector.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}
}
