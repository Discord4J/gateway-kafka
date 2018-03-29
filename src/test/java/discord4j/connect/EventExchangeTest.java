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

package discord4j.connect;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.common.jackson.PossibleModule;
import discord4j.common.json.payload.GatewayPayload;
import discord4j.common.json.payload.Heartbeat;
import discord4j.common.json.payload.Opcode;
import discord4j.connect.kafka.KafkaSink;
import discord4j.connect.kafka.KafkaSource;
import discord4j.gateway.GatewayClient;
import discord4j.gateway.payload.JacksonPayloadReader;
import discord4j.gateway.payload.JacksonPayloadWriter;
import discord4j.gateway.payload.PayloadReader;
import discord4j.gateway.payload.PayloadWriter;
import discord4j.gateway.retry.RetryOptions;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.time.Duration;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class EventExchangeTest {

	private String token;

	@Before
	public void initialize() {
		token = System.getenv("token");
	}

	@Test
	public void testForwardingGateway() {
		ObjectMapper mapper = new ObjectMapper()
				.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
				.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
				.registerModule(new PossibleModule());
		PayloadReader reader = new JacksonPayloadReader(mapper);
		PayloadWriter writer = new JacksonPayloadWriter(mapper);
		RetryOptions retryOptions = new RetryOptions(Duration.ofSeconds(5), Duration.ofSeconds(120));
		GatewayClient gatewayClient = new GatewayClient(reader, writer, retryOptions, token);

		SinkConnector sink = new KafkaSink("localhost:9092", "gateway-inbound");
		sink.send(gatewayClient.receiver().map(EventExchangeTest::toPayload))
				.subscribeOn(Schedulers.newSingle("kafka-producer"))
				.log().subscribe();

		SourceConnector source = new KafkaSource("localhost:9092", "gateway-outbound");
		source.receive(payload -> fromPayload(payload, mapper).map(gatewayClient.sender()::next).then())
				.log().subscribe();

		gatewayClient.execute("wss://gateway.discord.gg?v=6&encoding=json&compress=zlib-stream").block();
	}

	@Test
	public void testValueConversion() {
		ObjectMapper mapper = new ObjectMapper()
				.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
				.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
				.registerModule(new PossibleModule());
		GatewayPayload<?> gatewayPayload = GatewayPayload.heartbeat(new Heartbeat(123));
		Payload payload = toPayload(gatewayPayload);
		GatewayPayload<?> convertedPayload = mapper.convertValue(payload, GatewayPayload.class);
		assertEquals(Opcode.HEARTBEAT, convertedPayload.getOp());
		assertEquals(123, ((Heartbeat) convertedPayload.getData()).getSeq());
	}

    public static Mono<GatewayPayload> fromPayload(Payload payload, ObjectMapper mapper) {
        try {
            return Mono.just(mapper.convertValue(payload, GatewayPayload.class));
        } catch (IllegalArgumentException e) {
            return Mono.error(e);
        }
    }

    public static Payload toPayload(GatewayPayload<?> gatewayPayload) {
        Payload payload = new Payload();
        payload.setOp(Optional.ofNullable(gatewayPayload.getOp()).map(Opcode::getRawOp).orElse(0));
        payload.setData(gatewayPayload.getData());
        payload.setSequence(gatewayPayload.getSequence());
        payload.setType(gatewayPayload.getType());
        return payload;
    }
}
