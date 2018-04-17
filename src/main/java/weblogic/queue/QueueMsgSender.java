package weblogic.queue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Hashtable;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import weblogic.entity.User;
/**
 * 消息发送者
 * @author sdl
 *
 */
public class QueueMsgSender {
	//默认的 JNDI
	public final static String JNDI_FACTORY = "weblogic.jndi.WLInitialContextFactory";

	//weblogic地址
	public final static String PROVIDER_URL = "t3://localhost:7001";

	//创建的连接工厂的JNDI名称
	public final static String CONNECTION_FACTORY_JNDI_NAME = "connectionFactory";

	//创建的队列的JNDI名称
	public final static String QUEUE_JNDI_NAME = "myJMSQueueJNDIName";

	private QueueConnectionFactory qconFactory;
	private QueueConnection queueConnection;
	private QueueSession queueSession;
	private QueueSender queueSender;
	private Queue queue;
	private TextMessage textMessage;
	private StreamMessage streamMessage;
	private BytesMessage bytesMessage;
	private MapMessage mapMessage;
	private ObjectMessage objectMessage;

	//用于返回content
	private static InitialContext getInitialContext() throws NamingException {
		Hashtable table = new Hashtable();
		table.put(Context.INITIAL_CONTEXT_FACTORY, JNDI_FACTORY); 
		table.put(Context.PROVIDER_URL, PROVIDER_URL);
		InitialContext context = new InitialContext(table);
		return context;
	}

	//通过content和队列名称打开连接
	public void init(Context ctx, String queueName) throws NamingException, JMSException {
		qconFactory = (QueueConnectionFactory) ctx.lookup(CONNECTION_FACTORY_JNDI_NAME);
		queueConnection = qconFactory.createQueueConnection();
		queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		queue = (Queue) ctx.lookup(queueName);
		queueSender = queueSession.createSender(queue);

		textMessage = queueSession.createTextMessage();
		streamMessage = queueSession.createStreamMessage();
		bytesMessage = queueSession.createBytesMessage();
		mapMessage = queueSession.createMapMessage();
		objectMessage = queueSession.createObjectMessage();

		queueConnection.start();
	}

	//发送不同的短信
	public void send(String message) throws JMSException {
		// type1: set TextMessage
		textMessage.setText(message);

		// type2: set StreamMessage
		streamMessage.writeString(message);
		streamMessage.writeInt(20);

		// type3: set BytesMessage
		byte[] block = message.getBytes();
		bytesMessage.writeBytes(block);

		// type4: set MapMessage
		mapMessage.setString("name", message);

		// type5: set ObjectMessage
		User user = new User();
		user.setName(message);
		user.setAge(30);
		objectMessage.setObject(user);

		queueSender.send(textMessage);
	}

	//获取要发送的短信发送
	private static void readAndSend(QueueMsgSender msgSender ,QueueSession queueSession) throws IOException, JMSException {
		BufferedReader msgStream = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Enter message(input quit to quit):");  
		String line = null;
		boolean quit = false; 
		do {
			line = msgStream.readLine();
			if (line != null && line.trim().length() != 0) {
				msgSender.send(line);
				System.out.println("JMS Message Sent: " + line + "\n");
				quit = line.equalsIgnoreCase("quit");
			}
		} while (!quit);

	}

	/**
	 * 关闭连接
	 * 
	 * @exception JMSException if JMS fails to close objects due to internal error
	 */
	public void close() throws JMSException {
		queueSender.close();
		queueSession.close();
		queueConnection.close();
	}

	public static void main(String[] args) throws Exception {
		InitialContext ctx = getInitialContext(); 
		QueueMsgSender sender = new QueueMsgSender();  
		sender.init(ctx, QUEUE_JNDI_NAME);
		readAndSend(sender , sender.queueSession);
		sender.close();
	}
}
