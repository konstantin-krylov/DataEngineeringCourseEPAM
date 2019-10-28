package com.epam.postgesqltasks;

public interface DataLoader {

    void loadFromFile(String file, String tableName, boolean truncateBeforeLoad) throws Exception;
}
