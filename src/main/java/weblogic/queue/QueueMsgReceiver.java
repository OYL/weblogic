package weblogic.queue;

import java.util.Hashtable;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import weblogic.entity.User;
/**
 * 消息接受者
 * @author sdl
 *
 */
public class QueueMsgReceiver {
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
	private QueueReceiver queueReceiver;
	private Queue queue;
	private boolean quit = false;
	//用于返回content
	private static InitialContext getInitialContext() throws NamingException {
		Hashtable table = new Hashtable();
		table.put(Context.INITIAL_CONTEXT_FACTORY, JNDI_FACTORY);
		table.put(Context.PROVIDER_URL, PROVIDER_URL);
		InitialContext context = new InitialContext(table);
		return context;
	}
	//通过content和队列名称注册一个listener用于获取信息
	public void init(Context ctx, String queueName) throws NamingException, JMSException {
		qconFactory = (QueueConnectionFactory) ctx.lookup(CONNECTION_FACTORY_JNDI_NAME);
		queueConnection = qconFactory.createQueueConnection(); 
		queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		queue = (Queue) ctx.lookup(queueName);
		queueReceiver = queueSession.createReceiver(queue); 
		queueReceiver.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(Message msg) {
				try {
					System.out.println("收到一条消息： " + ((TextMessage)msg).getText());
				} catch (JMSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				onMessages(msg);
			}
		});
		// second thread: message reveive thread.
		queueConnection.start();  
	}

	/**
	 * 根据不同的信息类型输出不同的结果
	 * 
	 * @param message message
	 */
	public void onMessages(Message message) {
		try {
			String msgStr = "";  
			int age = 0; 

			if (message instanceof TextMessage) {
				msgStr = ((TextMessage) message).getText();
			} else if (message instanceof StreamMessage) {
				msgStr = ((StreamMessage) message).readString();
				age = ((StreamMessage) message).readInt();
			} else if (message instanceof BytesMessage) {
				byte[] block = new byte[1024];
				((BytesMessage) message).readBytes(block);
				msgStr = String.valueOf(block);
			} else if (message instanceof MapMessage) {
				msgStr = ((MapMessage) message).getString("name");
			} else if (message instanceof ObjectMessage) {
				User user = (User) ((ObjectMessage) message).getObject();
				msgStr = user.getName(); 
				age = user.getAge();
			}

			System.out.println("Message Received: " + msgStr + ", " + age);

			if (msgStr.equalsIgnoreCase("quit")) {
				synchronized (this) {
					quit = true;
					this.notifyAll(); // Notify main thread to quit
				}
			}
		} catch (JMSException e) {
			throw new RuntimeException("error happens", e);
		}
	}

	/**
	 * 关闭连接
	 * 
	 */
	public void close() throws JMSException {
		queueReceiver.close();
		queueSession.close();
		queueConnection.close();
	}

	public static void main(String[] args) throws Exception {
		InitialContext ctx = getInitialContext();
		QueueMsgReceiver receiver = new QueueMsgReceiver(); 
		receiver.init(ctx, QUEUE_JNDI_NAME);

		// Wait until a "quit" message has been received.
		synchronized (receiver) {
			while (!receiver.quit) {
				try {
					receiver.wait();
				} catch (InterruptedException e) { 
					throw new RuntimeException("error happens", e);
				}
			}
		}
		receiver.close();
	}
}
