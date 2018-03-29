/*
 * This file is part of Discord4J.
 *
 * Discord4J is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discord4J is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Discord4J. If not, see <http://www.gnu.org/licenses/>.
 */

package discord4j.connect.kafka;

import discord4j.connect.Payload;
import discord4j.connect.SourceConnector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class KafkaSource implements SourceConnector {

	private static final Logger log = Loggers.getLogger(KafkaSource.class);

	private final String topic;
	private final String bootstrapServers;
	private final Scheduler scheduler;
	private final String groupId = "gateway-source";

	public KafkaSource(String bootstrapServers, String topic) {
		this.bootstrapServers = bootstrapServers;
		this.topic = topic;
		this.scheduler = Schedulers.newSingle("kafka-consumer");
	}

	private ReceiverOptions<Integer, Payload> receiverOptions() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PayloadSerde.class);
		return ReceiverOptions.create(props);
	}

	private ReceiverOptions<Integer, Payload> receiverOptions(Collection<String> topics) {
		return receiverOptions()
				.addAssignListener(p -> log.info("Group {} partitions assigned {}", groupId, p))
				.addRevokeListener(p -> log.info("Group {} partitions revoked {}", groupId, p))
				.subscription(topics);
	}

	@Override
	public Flux<?> receive(Function<Payload, Mono<Void>> processor) {
		return KafkaReceiver.create(receiverOptions(Collections.singletonList(topic)).commitInterval(Duration.ZERO))
				.receive()
				.publishOn(scheduler)
				.concatMap(m -> processor.apply(m.value()).doOnSuccess(r -> m.receiverOffset().commit().block()))
				.retry()
				.doOnCancel(this::close);
	}

	@Override
	public void close() {
		scheduler.dispose();
	}
}
