package MessageQueue;

/**
 * Mock MessageQueue that can be subscribed to, uses RabbitMQ
 * Since mock, for unit testing assume all functions are perfect
 * @author Rob Fusco
 */
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

public class MessageQueueMock {
    private final static String QUEUE_NAME = "MQ";

    public interface CallBack {

        void callback_function(int user_id, String delta);
    }

    public void register(CallBack callback, int user_id, String delta) {
        callback.callback_function(user_id, delta);
    }
    /**
     * Waits until running thread is explicitly killed for any message posted to queue
     * @param callback
     * @throws Exception 
     */
    public void subscribe_to_updates(CallBack callback) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println("Subribed to Queue " + QUEUE_NAME + ". Waiting for updates.");

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(QUEUE_NAME, true, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            MessageQueueMock caller = new MessageQueueMock();
            caller.register(callback, Integer.parseInt(delivery.getProperties().getMessageId()), message);
        }
    }
}