package org.lsst.phosimingest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author tonyj
 */
public class Registry implements AutoCloseable {

    private final Connection conn;
    private final String createTable;
    private final String insertInto;

    Registry(Path folder, PhosimIngest.Options options) throws IOException {
        createTable = "CREATE TABLE " + (options.update ? " IF NOT EXISTS " : "");
        insertInto = "Insert " + (options.update ? " OR REPLACE " : "")+" INTO ";
        
        Path db = folder.resolve("registry.sqlite3");
        if (options.clobber) Files.deleteIfExists(db);
        try {
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:" + db.toString());
            conn.setAutoCommit(false);
            doTransaction(() -> {
                try (Statement stmt = conn.createStatement()) {
                    String sql = createTable + "raw (id integer primary key autoincrement, taiObs text,visit int,filter text,raft text,snap int,ccd text,sensor text,expTime double,channel text, unique(visit,sensor,raft,channel,snap))";
                    stmt.executeUpdate(sql);
                    sql = createTable + "raw_visit (visit int,filter text, unique(visit))";
                    stmt.executeUpdate(sql);
                }
            });
        } catch (ClassNotFoundException | SQLException x) {
            throw new IOException("Error opening sqlite database", x);
        }
    }

    void addVisit(Visit visit) throws IOException {
        try {
            doTransaction(() -> {
                String sql = insertInto + "\"raw\" VALUES(?,?,?,?,?,?,?,?,?,?)";
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setString(2, visit.getMJDString());
                    stmt.setInt(3, visit.getVisit());
                    stmt.setString(4, visit.getFilter());
                    stmt.setString(5, visit.getRaftName());
                    stmt.setInt(6, visit.getPairid());
                    stmt.setString(7, visit.getSensorName());
                    stmt.setString(8, visit.getSensorName());
                    stmt.setDouble(9, visit.getExptime());
                    stmt.setString(10, "0,0");
                    stmt.execute();
                }
                sql = insertInto + "\"raw_visit\" VALUES(?,?);";
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setInt(1, visit.getVisit());
                    stmt.setString(2, visit.getFilter());
                    stmt.execute();
                }
            });
        } catch (SQLException x) {
            throw new IOException("Error writing sqlite database", x);
        }
    }

    @Override
    public void close() throws SQLException {
        conn.close();
    }

    private void doTransaction(SQLRunnable runnable) throws SQLException {
        try {
            runnable.run();
        } catch (SQLException x) {
            conn.rollback();
            throw x;
        } finally {
            conn.commit();

        }
    }

    private interface SQLRunnable {

        void run() throws SQLException;
    }
}
