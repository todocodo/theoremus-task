package maven_project_dsl_streams;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class TransformJsonStream {
	
	 /**
     * A serde for any class that works with JSON - This specific class is for the json input message 
     *
     * @param <T> The concrete type of the class that gets de/serialized
     */
    public static class JSONSerde<T extends JSONSerdeCompatible> implements Serializer<T>, Deserializer<T>, Serde<T> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {}

        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(final String topic, final byte[] data) {
            if (data == null) {
                return null;
            }

            try {
                return (T) OBJECT_MAPPER.readValue(data, JSONSerdeCompatible.class);
            } catch (final IOException e) {
                throw new SerializationException(e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final T data) {
            if (data == null) {
                return null;
            }
            OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            try {
                return OBJECT_MAPPER.writeValueAsBytes(data);
            } catch (final Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public void close() {}

        @Override
        public Serializer<T> serializer() {
            return this;
        }

        @Override
        public Deserializer<T> deserializer() {
            return this;
        }
    }
    
	 /**
     * A second serde for the transformed json message - it does exactly the same thing as the first one
     *
     * @param <T> The concrete type of the class that gets de/serialized
     */
    public static class JSONSerdeNew<T extends NewJsonData> implements Serializer<T>, Deserializer<T>, Serde<T> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {}

        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(final String topic, final byte[] data) {
            if (data == null) {
                return null;
            }

            try {
                return (T) OBJECT_MAPPER.readValue(data, NewJsonData.class);
            } catch (final IOException e) {
                throw new SerializationException(e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final T data) {
            if (data == null) {
                return null;
            }
            OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            try {
                return OBJECT_MAPPER.writeValueAsBytes(data);
            } catch (final Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public void close() {}

        @Override
        public Serializer<T> serializer() {
            return this;
        }

        @Override
        public Deserializer<T> deserializer() {
            return this;
        }
    }


    // POJO classes - to store the json values into java variables so we can work with them
    
    static public class Data{
        @JsonProperty("date-time") 
        public DateTime dateTime;
        @JsonProperty("gps-info") 
        public GpsInfo gpsInfo;
        @JsonProperty("modem-info") 
        public ModemInfo modemInfo;
        @JsonProperty("stop-info") 
        public StopInfo stopInfo;
    }

    static public class DateTime{
        @JsonProperty("system") 
        public String system;
    }

    static public class GpsInfo{
        @JsonProperty("Altitude") 
        public String altitude;
        @JsonProperty("Date") 
        public String date;
        @JsonProperty("HDOP") 
        public String hDOP;
        @JsonProperty("Latitude") 
        public String latitude;
        @JsonProperty("Longitude") 
        public String longitude;
        @JsonProperty("SatelliteUsed") 
        public int satelliteUsed;
        @JsonProperty("Speed") 
        public double speed;
        @JsonProperty("Time") 
        public String time;
        @JsonProperty("Validity") 
        public String validity;
    }

    static  public class ModemInfo{
        @JsonProperty("signal-quality") 
        public String signalQuality;
    }

    static public class JSONSerdeCompatible{
        @JsonProperty("data") 
        public Data data;
        @JsonProperty("device-id") 
        public String deviceId;
        @JsonProperty("device-type") 
        public String deviceType;
        @JsonProperty("hostname")
        public String hostname;
        @JsonProperty("priority")
        public int priority;
        @JsonProperty("scheme-version") 
        public String schemeVersion;
        @JsonProperty("vehicle-id") 
        public String vehicleId;
    }

    static public class StopInfo{
    	
    }
    
    // New POJO Class - for the transformed json message
    static public class NewJsonData{
    	@JsonProperty("altitude") 
        public String altitude;
    	@JsonProperty("latitude") 
        public String latitude;
        @JsonProperty("longitude") 
        public String longitude;
        @JsonProperty("satellite_used") 
        public int satelliteUsed;
        @JsonProperty("speed") 
        public double speed;
        @JsonProperty("timestamp") 
        public int timestamp;
        @JsonProperty("vehicle_id") 
        public String vehicleId;
        @JsonProperty("device_id") 
        public String deviceId;
        @JsonProperty("signal_quality") 
        public String signalQuality;
    }
   

	  static final String inputTopic = "json-messages-input";
	  static final String outputTopic = "transformed-messages-output";
	  
	 public static void main(String[] args) {
	    	
	    	BasicConfigurator.configure();
	        Logger.getRootLogger().setLevel(Level.INFO);
	        
	        Properties props = getConfig();
	        
	        StreamsBuilder builder = new StreamsBuilder();

	        // Build the Topology
	        final KStream<String, JSONSerdeCompatible> messages = builder.stream(inputTopic, Consumed.with(Serdes.String(), new JSONSerde<>()));
	        
	        final KStream<String, NewJsonData> outputMessages = messages.mapValues((value) -> {
	        	
	        	final NewJsonData transformedMessage = new NewJsonData();
	        	transformedMessage.altitude = value.data.gpsInfo.altitude;
	        	transformedMessage.latitude = value.data.gpsInfo.latitude;
	        	transformedMessage.longitude = value.data.gpsInfo.longitude;
	        	transformedMessage.satelliteUsed = value.data.gpsInfo.satelliteUsed;
	        	transformedMessage.speed = value.data.gpsInfo.speed;
	        	transformedMessage.timestamp = (int) (System.currentTimeMillis() / 1000L);
	        	transformedMessage.vehicleId = value.vehicleId;
	        	transformedMessage.deviceId = value.deviceId;
	        	transformedMessage.signalQuality = value.data.modemInfo.signalQuality;
	        	
	        	return transformedMessage;
	        });
//	        
	        outputMessages.to(outputTopic, Produced.with(Serdes.String(), new JSONSerdeNew<>()));
	        
	        // Create the Kafka Streams Application
	        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
	        // Start the application
	        kafkaStreams.start();

	        // attach shutdown handler to catch control-c
	        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	    }

	 	// Basic properties - exept from the default key and value serde which uses the json one created above
	    private static Properties getConfig() {
	        Properties properties = new Properties();
	        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-json");
	        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JSONSerde.class);
	        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);
	        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

	        return properties;
	    }

}
