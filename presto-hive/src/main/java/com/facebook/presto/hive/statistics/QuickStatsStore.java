package com.facebook.presto.hive.statistics;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.cache.Cache;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class QuickStatsStore
{
    public static final Logger log = Logger.get(QuickStatsStore.class);

    private static final String DB_PATH = "/tmp/quickstats.db";
    private static final String JDBC_URL = "jdbc:sqlite:" + DB_PATH;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final String QUICK_STATS_TABLE = "quick_stats";
    private static final String STATS_COLUMN_NAME = "value";
    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS " + QUICK_STATS_TABLE + " (" +
            "  key TEXT PRIMARY KEY, " +
            STATS_COLUMN_NAME + " BLOB" +
            ")";

    private static final String INSERT_SQL = "INSERT OR REPLACE INTO " + QUICK_STATS_TABLE + " (key, " + STATS_COLUMN_NAME + ") VALUES (?, ?)";
    private static final String SELECT_ALL_SQL = "SELECT key, " + STATS_COLUMN_NAME + " FROM " + QUICK_STATS_TABLE;

    static {
        OBJECT_MAPPER.registerModule(new Jdk8Module());
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
    }

    private static void loadDriverIfNotLoaded()
    {
        try {
            URL url = new File("/yoda/.m2/repository/org/xerial/sqlite-jdbc/3.51.1.0/sqlite-jdbc-3.51.1.0.jar").toURI().toURL();
            URLClassLoader loader = new URLClassLoader(new URL[]{url}, ClassLoader.getSystemClassLoader());
            Class<?> clazz = loader.loadClass("org.sqlite.JDBC");
            Class.forName("org.sqlite.JDBC");
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to load SQLite JDBC driver", e);
        }
    }

    public static void loadInto(Cache<String, PartitionStatistics> cache)
    {
        ensureTableExists();

        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(SELECT_ALL_SQL)) {

            while (rs.next()) {
                String key = rs.getString("key");
                byte[] valueBytes = rs.getBytes(STATS_COLUMN_NAME);

                // Only load if not already in cache
                if (cache.getIfPresent(key) == null) {
                    try {
                        PartitionStatistics stats = OBJECT_MAPPER.readValue(valueBytes, PartitionStatistics.class);
                        cache.put(key, stats);
                    }
                    catch (IOException e) {
                        // Log and skip malformed entries
                        log.error(e, "Failed to deserialize partition statistics for key: %s", key);
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to load partition statistics from SQLite", e);
        }
    }

    public static void storeFrom(Map<String, PartitionStatistics> cache)
    {
        ensureTableExists();

        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             PreparedStatement statement = conn.prepareStatement(INSERT_SQL)) {

            conn.setAutoCommit(false);

            for (var entry : cache.entrySet()) {
                String key = entry.getKey();
                PartitionStatistics stats = entry.getValue();

                try {
                    byte[] valueBytes = OBJECT_MAPPER.writeValueAsBytes(stats);
                    statement.setString(1, key);
                    statement.setBytes(2, valueBytes);
                    statement.addBatch();
                }
                catch (IOException e) {
                    // Log and skip entries that can't be serialized
                    log.error(e, "Failed to serialize partition statistics for key: %s", key);
                }
            }

            statement.executeBatch();
            conn.commit();
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to store partition statistics to SQLite", e);
        }
    }

    private static void ensureTableExists()
    {
        try {
            loadDriverIfNotLoaded();
        }
        catch (Exception e) {
            System.err.println("Failed to load driver");
        }
        // Ensure parent directory exists
        Path dbPath = Paths.get(DB_PATH);
        try {
            Files.createDirectories(dbPath.getParent());
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to create directory for SQLite database", e);
        }

        // Create table if it doesn't exist
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute(CREATE_TABLE_SQL);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to create SQLite table", e);
        }
    }
}
