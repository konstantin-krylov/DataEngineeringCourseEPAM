package com.epam.postgesqltasks;

import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.*;

public class BasicConnectionPoolTest {

    @Test
    public void whenCalledgetConnection_thenCorrect() throws SQLException {
        ConnectionPool connectionPool = BasicConnectionPool
                .create("jdbc:h2:mem:test", "user", "password");

        assertTrue(connectionPool.getConnection().isValid(1));
    }

    @Test
    public void whenCalledGetConnectionPostgres_thenCorrect() throws SQLException {
        ConnectionPool connectionPool = BasicConnectionPool
                .create("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");

        assertFalse(connectionPool.getConnection().isClosed());
    }
}