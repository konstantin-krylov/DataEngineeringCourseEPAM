package com.epam.postgesqltasks;

import com.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.util.Arrays;

public class CSVLoader implements DataLoader {

    private static final String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
    private static final String TABLE_REGEX = "\\$\\{table\\}";
    private static final String KEYS_REGEX = "\\$\\{keys\\}";
    private static final String VALUES_REGEX = "\\$\\{values\\}";

    private Connection connection;
    private char separator;

    public CSVLoader(Connection connection, char separator) {
        this.connection = connection;
        this.separator = separator;
    }

    @Override
    public void loadFromFile(String csvFile, String tableName, boolean truncateBeforeLoad) throws Exception {
        CSVReader csvReader = null;

        if (connection == null) {
            throw new Exception("Not valid connection");
        }

        csvReader = new CSVReader(new FileReader(csvFile), this.separator);
        String[] headerRow = csvReader.readNext();
        System.out.println(Arrays.toString(headerRow));

        if (headerRow == null) {
            throw new FileNotFoundException("No columns defined in given CSV file." +
                    "Please check the CSV file format.");
        }
        // generate String "?,?,?,?,?...."
        String questionmarks = StringUtils.repeat("?,", headerRow.length);
        questionmarks = (String) questionmarks.subSequence(0, questionmarks.length() - 1);

        String query = SQL_INSERT.replaceFirst(TABLE_REGEX, tableName);
        query = query.replaceFirst(KEYS_REGEX, StringUtils.join(headerRow, ","));
        query = query.replaceFirst(VALUES_REGEX, questionmarks);

        System.out.println("Query: " + query);

        String[] nextLine;

        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = this.connection;
            con.setAutoCommit(false);
            ps = con.prepareStatement(query);

            if (truncateBeforeLoad) {
                // TODO create table with parameters
                String sqlQueryCreateTable = "DROP TABLE IF EXISTS customer CASCADE;" +
                        "CREATE TABLE customer (";
                for (int i = 0; i <headerRow.length ; i+=2) {
                    sqlQueryCreateTable = sqlQueryCreateTable.concat(headerRow[i].concat(" VARCHAR(100),"));
                }
                sqlQueryCreateTable = (String) sqlQueryCreateTable.subSequence(0, sqlQueryCreateTable.length() - 1);
                sqlQueryCreateTable+=")";
                System.out.println(sqlQueryCreateTable);
                con.createStatement().execute(sqlQueryCreateTable);
                //delete data from table before loading csv
                con.createStatement().execute("DELETE FROM " + tableName);
            }

            final int batchSize = 1000;
            int count = 0;
            Date date;
            while ((nextLine = csvReader.readNext()) != null) {
                int index = 1;
                for (String string : nextLine) {
                    date = (Date) DateUtil.convertToDate(string);
                    if (null != date) {
                        ps.setDate(index++, new java.sql.Date(date
                                .getTime()));
                    } else {
                        ps.setString(index++, string);
                    }
                }
                ps.addBatch();

                if (++count % batchSize == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch(); // insert remaining records
            con.commit();
        } catch (Exception e) {
            con.rollback();
            e.printStackTrace();
            throw new Exception(
                    "Error occured while loading data from file to database."
                            + e.getMessage());
        } finally {
            if (null != ps)
                ps.close();
            if (null != con)
                con.close();

            csvReader.close();
        }
    }

    public char getSeparator() {
        return separator;
    }

    public void setSeparator(char separator) {
        this.separator = separator;
    }
}
