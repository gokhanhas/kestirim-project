package kestirim;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Sensor {
	/**
	 * Main method to simulate sensor data generation and send it to a Kafka topic.
	 * This program generates random sensor data, calculates the azimuth angle between
	 * the sensor and a fixed target location, and sends the data to a Kafka topic.
	 *
	 * @param args An array of command-line arguments. The first argument (args[0]) is expected
	 *             to be the sensor ID, which uniquely identifies the sensor.
	 */
	public static void main(String[] args) {
		String sensorId = args[0];
        String topic = "quickstart-events";        
        
        // Kafka producer configuration properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
          
        try {
        	while(true) { 
        		double sensorX = getRandomCoordinate(-500, 500);
        		double sensorY = getRandomCoordinate(-500, 500);
        		
        		// Target
        		double targetX = -1;
        		double targetY = 5;
        		
                // Calculate the azimuth angle between the sensor and the target
        		double azimuth = calculateAzimuth(sensorX, sensorY, targetX, targetY);
        		
                // Create a sensor data message with sensor information and azimuth angle
        		String sensorData = "Sensor= " + sensorId + " X= " + sensorX + " Y= " + sensorY + " degrees= " + azimuth;
        		producer.send(new ProducerRecord<>(topic, sensorData));
        		producer.flush();
        	}
        } catch(Exception e) {
        	e.printStackTrace();
        } finally {
        	producer.close();
        }
	}
	
	/**
	 * Generates a random double-precision floating-point number within the specified range.
	 *
	 * @param min The minimum value of the range (inclusive).
	 * @param max The maximum value of the range (inclusive).
	 * @return A random double value between {@code min} (inclusive) and {@code max} (inclusive).
	 */
	private static double getRandomCoordinate(double min, double max) {
		return min + (new Random().nextDouble() * (max - min +1));
	}
	
	/**
	 * Calculates the azimuth angle in degrees between a sensor location and a target location.
	 *
	 * Azimuth represents the angle measured clockwise from the north direction (0 degrees) to
	 * the direction pointing towards the target location. This method computes the azimuth
	 * based on the sensor's coordinates (sensorX, sensorY) and the target's coordinates (targetX, targetY).
	 *
	 * @param sensorX The X-coordinate of the sensor's location.
	 * @param sensorY The Y-coordinate of the sensor's location.
	 * @param targetX The X-coordinate of the target's location.
	 * @param targetY The Y-coordinate of the target's location.
	 * @return The azimuth angle in degrees, where 0 degrees represents north and angles increase clockwise.
	 */
	private static double calculateAzimuth(double sensorX, double sensorY, double targetX, double targetY) {
		 // Calculate the change in X and Y coordinates.
		double deltaX = targetX - sensorX;
		double deltaY = targetY - sensorY;
		
		// Calculate the tangent of the angle formed by deltaY and deltaX.
		double tan = deltaY / deltaX;
		
		 // Calculate the azimuth angle in radians and convert it to degrees.
		double azimuthInDegrees = Math.atan(tan) * 180 / Math.PI;

		// Handle special cases when deltaX is 0 or deltaY is negative.
		if(deltaX == 0) {
			azimuthInDegrees = sensorY < targetY ? 90 : 270;
		} else if(deltaY < 0) {
			azimuthInDegrees += 180;
		}
		
		// Ensure that the azimuth angle is within the range [0, 360) degrees.
		if(azimuthInDegrees < 0)
			azimuthInDegrees += 360;
		
		return azimuthInDegrees;
	}
	
	// zookeeper-server-start.sh ~/kafka_2.13-3.5.1/config/zookeeper.properties
	// kafka-server-start.sh ~/kafka_2.13-3.5.1/config/server.properties

}
