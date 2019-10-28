package com.epam.postgesqltasks;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Main {

    private static final String CSV_FILE = "C:\\Users\\Konstantin_Krylov2\\IdeaProjects\\DataEngineeringCourseEPAM\\src\\main\\resources\\P9-ConsumerComplaints.csv";

    public static void main(String[] args) throws SQLException {
        String user = "postgres";
        String password = "postgres";
        String url = "jdbc:postgresql://localhost:5432/postgres";

        BasicConnectionPool basicConnectionPool = BasicConnectionPool.create(url,user,password);

        try (Connection con = basicConnectionPool.getConnection()) {
            String query = "";
            PreparedStatement preparedStatement = con.prepareStatement(query);
            preparedStatement.execute();

            DataLoader loader = new CSVLoader(con, ',');
            loader.loadFromFile(CSV_FILE, "CUSTOMER", true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
