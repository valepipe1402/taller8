/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiFareSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your appliation into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {

		// read parameters
		//ParameterTool params = ParameterTool.fromArgs(args);
		//String input = params.getRequired("input");

		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(
				new CheckpointedTaxiFareSource("/home/bigdata/nycTaxiFares.gz", servingSpeedFactor));

		// compute tips per hour for each driver
		DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
				.keyBy((TaxiFare fare) -> fare.driverId)
				.timeWindow(Time.hours(1))
				.apply(new AddTips());

		// find the highest total tips in each hour
		DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
				.timeWindowAll(Time.hours(1))
				.maxBy(2);

		// print the result on stdout
		hourlyMax.print();

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

	/**
	 * Adds up the tips.
	 */
	public static class AddTips implements WindowFunction<
			TaxiFare, // input type
			Tuple3<Long, Long, Float>, // output type
			Long, // key type
			TimeWindow> // window type
	{

		@Override
		public void apply(
				Long key,
				TimeWindow window,
				Iterable<TaxiFare> fares,
				Collector<Tuple3<Long, Long, Float>> out) throws Exception {

			float sumOfTips = 0;
			for(TaxiFare fare : fares) {
				sumOfTips += fare.tip;
			}

			out.collect(new Tuple3<>(window.getEnd(), key, sumOfTips));
		}
	}


}
