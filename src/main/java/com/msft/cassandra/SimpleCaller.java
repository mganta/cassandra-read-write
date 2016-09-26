package com.msft.cassandra;

import com.datastax.driver.core.*;

public class SimpleCaller {

    public static void main(String [] args) {
        Cluster cluster;
        Session session;

        cluster = Cluster.builder().addContactPoint("52.163.63.183").build();
        session = cluster.connect("skippy");

        SimpleStatement statement = new SimpleStatement("INSERT INTO users (lastname, age, city, email, firstname) VALUES ('Jones', 35, 'Austin', 'bob@example.com', 'Bob')");
        statement.setConsistencyLevel(ConsistencyLevel.ANY);
        session.execute(statement);

        statement = new SimpleStatement("INSERT INTO users (lastname, age, city, email, firstname) VALUES ('Jones1', 35, 'Austin1', 'bob@example.com', 'Bob')");
        statement.setConsistencyLevel(ConsistencyLevel.ANY);
        session.execute(statement);

        // Use select to get the user we just entered
        statement = new SimpleStatement("SELECT * FROM users WHERE lastname='Jones'");
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        ResultSet results = session.execute(statement);
        for (Row row : results) {
            System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
        }


        // Update the same user with a new age
        statement = new SimpleStatement("update users set age = 36 where lastname = 'Jones'");
        statement.setConsistencyLevel(ConsistencyLevel.ANY);
        session.execute(statement);

      // Select and show the change
        statement = new SimpleStatement("select * from users where lastname='Jones'");
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        session.execute(statement);
        results = session.execute(statement);
        for (Row row : results) {
            System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));

        }

        statement = new SimpleStatement("DELETE FROM users WHERE lastname = 'Jones'");
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        session.execute(statement);
        // Show that the user is gone
        statement = new SimpleStatement("SELECT * FROM users");
        statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        results = session.execute(statement);
        for (Row row : results) {
            System.out.format("%s %d %s %s %s\n", row.getString("lastname"), row.getInt("age"),  row.getString("city"), row.getString("email"), row.getString("firstname"));
        }
        cluster.close();
    }
}





