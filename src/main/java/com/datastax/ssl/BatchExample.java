package com.datastax.ssl;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class BatchExample {
	
	private Session session;
	private static String keyspaceName = "test_keyspace";
	private static String tableName = keyspaceName + ".test_table";
	
	public BatchExample(){
			
		Cluster cluster = Cluster.builder().addContactPoint("localhost").build();		
		this.session = cluster.connect();
		
		System.out.println("Cluster and Session created.");
		
		this.setUp();
		this.insertBatch();
		this.tearDown();
		
		System.out.println("Batch test finished.");
		
		cluster.shutdown();
	}
	
	public void setUp(){
		
		//Set up Keyspace
		String createKeyspace = "CREATE KEYSPACE " + keyspaceName + " WITH replication = { "
				+ "'class': 'SimpleStrategy', 'replication_factor': '1' }";

		//Set up ColumnFamily
		String createTable = "CREATE TABLE " + tableName + "(user_id text PRIMARY KEY, first text, last text, city text, email text)";
				
		this.session.execute("DROP KEYSPACE IF EXISTS " + keyspaceName);		
		this.session.execute(createKeyspace);
		System.out.println("Keyspace " + keyspaceName + " created");
		
		this.session.execute(createTable);
		System.out.println("Table " + tableName + " created");		
	}
	
	public void insertBatch(){
		PreparedStatement insert = session.prepare("INSERT INTO " +tableName + "(user_id, first, last, city, email) VALUES (?, ?, ?, ?, ?)");
		
		PreparedStatement update = session.prepare("UPDATE " +tableName + " SET email = ? where user_id = ?");
		
		BatchStatement batch = new BatchStatement();
		batch.add(insert.bind("0001", "Harry", "Callaghan", "London", "harry@london.com"));
		batch.add(insert.bind("0002", "Jim", "Seiger", "New Hampshire", "jim@hampshire.com"));
		batch.add(insert.bind("0003", "Tony", "Jaane", "Paris", "tony@paris.com"));
		
		batch.add(update.bind("anthony@paris.com", "0003"));		
		session.execute(batch);
	}
	
	public void tearDown(){
		
		String dropKeyspace = "DROP KEYSPACE " + this.keyspaceName;
		
		this.session.execute(dropKeyspace);
		System.out.println("Keyspace DROPPED");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new BatchExample();
	}

}
