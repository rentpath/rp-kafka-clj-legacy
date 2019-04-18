// The problem: GenericAvroSerde doesn't handle primitive types (like KafkaAvroSerializer and KafkaAvroDeserializer do).
// In other words, GenericAvroSerde cannot be used with a primitive key schema like "string" or "long"; it only works with GenericData records and null.
// This solution comes from https://stackoverflow.com/questions/51955921/serde-class-for-avro-primitive-type
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class GenericPrimitiveAvroSerde<T> implements Serde<T> {

    private final Serde<Object> inner;

    /**
     * Constructor used by Kafka Streams.
     */
    public GenericPrimitiveAvroSerde() {
        inner = Serdes.serdeFrom(new KafkaAvroSerializer(), new KafkaAvroDeserializer());
    }

    public GenericPrimitiveAvroSerde(SchemaRegistryClient client) {
        this(client, Collections.emptyMap());
    }

    public GenericPrimitiveAvroSerde(SchemaRegistryClient client, Map<String, ?> props) {
        inner = Serdes.serdeFrom(new KafkaAvroSerializer(client), new KafkaAvroDeserializer(client, props));
    }

    @Override
    public void configure(final Map<String, ?> serdeConfig, final boolean isSerdeForRecordKeys) {
        inner.serializer().configure(serdeConfig, isSerdeForRecordKeys);
        inner.deserializer().configure(serdeConfig, isSerdeForRecordKeys);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();

    }

    @SuppressWarnings("unchecked")
    @Override
    public Serializer<T> serializer() {
        Object obj = inner.serializer();
        return (Serializer<T>) obj;

    }

    @SuppressWarnings("unchecked")
    @Override
    public Deserializer<T> deserializer() {
        Object obj = inner.deserializer();
        return (Deserializer<T>) obj;

    }

}
