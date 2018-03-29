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
import discord4j.connect.SinkConnector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class KafkaSink implements SinkConnector {

	private static final Logger log = Loggers.getLogger(KafkaSink.class);

	private final String topic;
	private final String bootstrapServers;
	private final KafkaSender<Integer, Payload> sender;

	public KafkaSink(String bootstrapServers, String topic) {
		this.bootstrapServers = bootstrapServers;
		this.topic = topic;
		this.sender = KafkaSender.create(
				senderOptions().producerProperty(ProducerConfig.ACKS_CONFIG, "all")
						.producerProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MAX_VALUE)
						.producerProperty(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE));
	}

	private SenderOptions<Integer, Payload> senderOptions() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PayloadSerde.class);
		return SenderOptions.create(props);
	}

	@Override
	public Flux<?> send(Flux<Payload> source) {
		return sender.send(source.map(p -> SenderRecord.create(new ProducerRecord<>(topic, sequence(p), p), sequence(p))))
				.doOnError(e -> log.error("Send failed, terminating.", e))
				.doOnNext(r -> {
					int id = r.correlationMetadata();
					log.trace("Successfully stored gateway with seq {} in Kafka", id);
				})
				.doOnCancel(this::close);
	}

	private Integer sequence(Payload payload) {
		return Optional.ofNullable(payload.getSequence()).orElse(0);
	}

	@Override
	public void close() {
		if (sender != null) {
			sender.close();
		}
	}
}
