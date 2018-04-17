package weblogic.topic;

import java.util.Hashtable;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicRequestor;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import weblogic.entity.User;
import weblogic.queue.QueueMsgReceiver;

public class TopicMsgSubscriber {
	//默认的 JNDI
		public final static String JNDI_FACTORY = "weblogic.jndi.WLInitialContextFactory";

		//weblogic地址
		public final static String PROVIDER_URL = "t3://localhost:7001";

		//创建的连接工厂的JNDI名称
		public final static String CONNECTION_FACTORY_JNDI_NAME = "connectionFactory";

		//创建的队列的JNDI名称
		public final static String TOPIC_JNDI_NAME = "myJMSTopicJNDIName";

		private TopicConnectionFactory tconFactory;
		private TopicConnection topicConnection;
		private TopicSession topicSession;
		private TopicSubscriber topicSubscriber;
		private Topic topic;
		private boolean quit = false;
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
			tconFactory = (TopicConnectionFactory) ctx.lookup(CONNECTION_FACTORY_JNDI_NAME);
			topicConnection = tconFactory.createTopicConnection();
			topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
			topic = (Topic) ctx.lookup(queueName);
			topicSubscriber = topicSession.createSubscriber(topic);

			topicSubscriber.setMessageListener(new MessageListener() {
				
				@Override
				public void onMessage(Message msg) {
					// TODO Auto-generated method stub
					try {
						System.out.println("topic1收到一条消息： " + ((TextMessage)msg).getText());
					} catch (JMSException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					onMessages(msg);
				}
			});
			topicConnection.start();
		}
		
		//通过content和队列名称打开连接
				public void init2(Context ctx, String queueName) throws NamingException, JMSException {
					tconFactory = (TopicConnectionFactory) ctx.lookup(CONNECTION_FACTORY_JNDI_NAME);
					topicConnection = tconFactory.createTopicConnection();
					topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
					topic = (Topic) ctx.lookup(queueName);
					topicSubscriber = topicSession.createSubscriber(topic);

					topicSubscriber.setMessageListener(new MessageListener() {
						
						@Override
						public void onMessage(Message msg) {
							// TODO Auto-generated method stub
							try {
								System.out.println("topic2收到一条消息： " + ((TextMessage)msg).getText());
							} catch (JMSException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							onMessages(msg);
						}
					});
					topicConnection.start();
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
			topicSubscriber.close();
			topicSession.close();
			topicConnection.close();
		}

		public static void main(String[] args) throws Exception {
			InitialContext ctx = getInitialContext();
			TopicMsgSubscriber subscriber = new TopicMsgSubscriber(); 
			subscriber.init(ctx, TOPIC_JNDI_NAME);
			subscriber.init2(ctx, TOPIC_JNDI_NAME);
			// Wait until a "quit" message has been received.
			synchronized (subscriber) {
				while (!subscriber.quit) {
					try {
						subscriber.wait();
					} catch (InterruptedException e) { 
						throw new RuntimeException("error happens", e);
					}
				}
			}
			subscriber.close();
		}
}
