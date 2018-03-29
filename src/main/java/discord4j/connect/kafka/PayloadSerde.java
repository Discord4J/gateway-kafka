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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import discord4j.connect.Payload;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class PayloadSerde implements Serializer<Payload>, Deserializer<Payload> {

	private final ObjectMapper mapper = new ObjectMapper();

	@Override
	public Payload deserialize(String topic, byte[] data) {
		try {
			return mapper.readValue(data, Payload.class);
		} catch (IOException e) {
			return new Payload();
		}
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public byte[] serialize(String topic, Payload data) {
		try {
			return mapper.writeValueAsBytes(data);
		} catch (JsonProcessingException e) {
			return new byte[0];
		}
	}

	@Override
	public void close() {

	}
}
