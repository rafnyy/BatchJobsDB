package MessageQueue;

/**
 * Mock send method that uses RabbitMQ to send a message
 * Since mock, for unit testing assume all functions are perfect
 * @author Rob Fusco
 */
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.util.Date;

public class SendMock {
    private final static String QUEUE_NAME = "MQ";
    
    public static void send(int user_id, String delta) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
       
        BasicProperties.Builder propsBuilder = new BasicProperties.Builder();
        propsBuilder
                //use message id to pass user_id
                .messageId(""+user_id)
                .timestamp(new Date())
                .contentType("application/json");

        channel.basicPublish("", QUEUE_NAME, propsBuilder.build(), delta.getBytes());
        System.out.println(" [x] Sent " + delta);
        
        channel.close();
        connection.close();
    }
}