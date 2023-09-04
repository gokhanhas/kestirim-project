package kestirim;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class CentralUnit {
	/**
	 * Main method for consuming Kafka messages, processing sensor data, and calculating target coordinates.
	 *
	 * This program subscribes to a Kafka topic, receives sensor data messages, processes the data to
	 * extract sensor information (ID, X-coordinate, Y-coordinate, and degrees), and calculates the
	 * target coordinates based on pairs of received sensor data points. It continuously listens for
	 * incoming messages and performs calculations when two sensor data points are received.
	 *
	 * @param args An array of command-line arguments (not used in this program).
	 */
	public static void main(String[] args) {
		String bootstrapServer = "localhost:9092";
        String topic = "quickstart-events";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            List<Double> xValues = new ArrayList<>();
            List<Double> yValues = new ArrayList<>();
            List<Double> degreesValues = new ArrayList<>();
            
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                String[] parts = value.split("\\s+");
                String sensorValue = "";
                double xValue = 0;
                double yValue = 0;
                double degreesValue = 0;

                for (int i = 0; i < parts.length; i++) {
                    if (parts[i].equals("Sensor=")) {
                        sensorValue = parts[i + 1];
                    } else if (parts[i].equals("X=")) {
                        xValue = Double.valueOf(parts[i + 1]);
                    } else if (parts[i].equals("Y=")) {
                        yValue = Double.valueOf(parts[i + 1]);
                    } else if (parts[i].equals("degrees=")) {
                        degreesValue = Double.valueOf(parts[i + 1]);
                    }
                }
                
               System.out.println("Sensor= " + sensorValue);
               System.out.println("X= " + xValue);
               System.out.println("Y= " + yValue);
               System.out.println("degrees= " + degreesValue);
               System.out.println("*************************");
                
               xValues.add(xValue);
               yValues.add(yValue);
               degreesValues.add(degreesValue);
                
               // Calculate target coordinates when two sensor data points are received 
               if(xValues.size() >= 2) {
            	   double x = calculateX(xValues.get(0), yValues.get(0), degreesValues.get(0), xValues.get(1), yValues.get(1), degreesValues.get(1));
                   double y = calculateY(xValues.get(0), yValues.get(0), degreesValues.get(0), xValues.get(1), yValues.get(1), degreesValues.get(1));

                   System.out.println("Hedef nokta: (" + x + ", " + y + ")");
                   System.out.println("*************************\n");
                   
                   xValues.clear();
                   yValues.clear();
                   degreesValues.clear();
               }
            }
        }
	}
	
	
	/**
	 * Calculates the X-coordinate of the point of intersection between two lines, defined by
	 * their starting points (x1, y1) and (x2, y2), and their respective azimuth angles (azimuth1, azimuth2).
	 * 
	 * The method computes the X-coordinate of the intersection point of two lines represented by their starting
	 * points and the angles at which they are oriented. It is assumed that the lines are infinite in length.
	 * 
	 * @param x1 The X-coordinate of the starting point of the first line.
	 * @param y1 The Y-coordinate of the starting point of the first line.
	 * @param azimuth1 The azimuth angle in degrees for the first line, measured clockwise from the north direction.
	 * @param x2 The X-coordinate of the starting point of the second line.
	 * @param y2 The Y-coordinate of the starting point of the second line.
	 * @param azimuth2 The azimuth angle in degrees for the second line, measured clockwise from the north direction.
	 * @return The X-coordinate of the point where the two lines intersect.
	 */
    private static double calculateX(double x1, double y1, double azimuth1, double x2, double y2, double azimuth2) {
        double tan1 = Math.tan(Math.toRadians(azimuth1));
        double tan2 = Math.tan(Math.toRadians(azimuth2));
        double x = ((y2 - y1) + (tan1 * x1) - (tan2 * x2)) / (tan1 - tan2);
        return x;
    }
    
    /**
     * Calculates the Y-coordinate of the point of intersection between two lines, defined by
     * their starting points (x1, y1) and (x2, y2), and their respective azimuth angles (azimuth1, azimuth2).
     *
     * The method computes the Y-coordinate of the intersection point of two lines represented by their starting
     * points and the angles at which they are oriented. It is assumed that the lines are infinite in length.
     *
     * @param x1 The X-coordinate of the starting point of the first line.
     * @param y1 The Y-coordinate of the starting point of the first line.
     * @param azimuth1 The azimuth angle in degrees for the first line, measured clockwise from the north direction.
     * @param x2 The X-coordinate of the starting point of the second line.
     * @param y2 The Y-coordinate of the starting point of the second line.
     * @param azimuth2 The azimuth angle in degrees for the second line, measured clockwise from the north direction.
     * @return The Y-coordinate of the point where the two lines intersect.
     */
    private static double calculateY(double x1, double y1, double azimuth1, double x2, double y2, double azimuth2) {
        double tan1 = Math.tan(Math.toRadians(azimuth1));
        double tan2 = Math.tan(Math.toRadians(azimuth2));
        double y = y1 + tan1 * (calculateX(x1, y1, azimuth1, x2, y2, azimuth2) - x1);
        return y;
    }
}
