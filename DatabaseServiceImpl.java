package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {

        return Arrays.asList(12212251);
    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords

    ) {
        try (Connection conn = dataSource.getConnection()) {


       create_danmu_table(conn, danmuRecords);
        create_danmu_like_table(conn, danmuRecords);
        create_user_table(conn, userRecords);
        create_and_insert_qq_wechat_tables(conn, userRecords);
        create_following_table(conn, userRecords);
         create_video_table(conn, videoRecords);
          create_video_like_table(conn, videoRecords);
          create_video_coin_table(conn, videoRecords);
         create_video_favorite_table(conn, videoRecords);
         create_viewer_duration_table(conn, videoRecords);
          create_and_insert_mid_max_table(conn);


        } catch (SQLException e) {
            System.err.println("SQL State: " + e.getSQLState());
            System.err.println("Error Code: " + e.getErrorCode());
            System.err.println("Message: " + e.getMessage());
            System.err.println("Error Location: " + e.getStackTrace()[0]);
            throw new RuntimeException("Failed to import data.", e);
        }
    }

    private void create_danmu_table(Connection conn, List<DanmuRecord> danmuRecords) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS danmu (" +
                        "danmu_id SERIAL PRIMARY KEY," +  // 新增列 danmu_id，并将其设为唯一主键
                        "bv VARCHAR ," +
                        "mid BIGINT," +
                        "time FLOAT," +
                        "content VARCHAR," +
                        "post_time TIMESTAMP," +
                        "test BIGINT" +
                        ")"
        )) {
            stmt.executeUpdate();

            try (PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO danmu (bv, mid, time, content, post_time) VALUES (?, ?, ?, ?, ?)"
            )) {
                for (DanmuRecord danmuRecord : danmuRecords) {
                    insertStmt.setString(1, danmuRecord.getBv());
                    insertStmt.setLong(2, danmuRecord.getMid());
                    insertStmt.setFloat(3, danmuRecord.getTime());
                    insertStmt.setString(4, danmuRecord.getContent());
                    insertStmt.setTimestamp(5, danmuRecord.getPostTime());
                    insertStmt.addBatch();
                }
                insertStmt.executeBatch();
            }
        }
    }


    private void create_danmu_like_table(Connection conn, List<DanmuRecord> danmuRecords) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS danmu_like (" +
                        "danmu_id BIGINT," +
                        "liked_by BIGINT," +
                        "bv VARCHAR," +
                        "CONSTRAINT danmu_like_pk PRIMARY KEY (danmu_id, liked_by)" +
                        ")"
        )) {
            stmt.executeUpdate();

            try (PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO danmu_like (danmu_id, liked_by,bv) VALUES (?, ?,?)"
            )) {
                long danmuIdCounter = 1;  // 初始的 danmu_id 值

                for (DanmuRecord danmuRecord : danmuRecords) {
                    for (Long likedBy : danmuRecord.getLikedBy()) {
                        insertStmt.setLong(1, danmuIdCounter);
                        insertStmt.setLong(2, likedBy);
                        insertStmt.setString(3, danmuRecord.getBv());
                        insertStmt.addBatch();
                    }
                    danmuIdCounter++;
                }
                insertStmt.executeBatch();
            }
        }
    }


    private static void create_and_insert_mid_max_table(Connection connection) throws SQLException {
        String create_table_sql = "CREATE TABLE IF NOT EXISTS mid_max (" +
                "mid_name VARCHAR(255) PRIMARY KEY," +
                "mid_value BIGINT);";

        String insert_data_sql = "INSERT INTO mid_max (mid_name, mid_value) VALUES (?, ?)";

        long max_mid_value = 0;
        long max_danmu_mid_value = 0;
        long max_video_mid_value = 0;

        // Get max mid value from user_basic table
        String select_max_mid_sql = "SELECT MAX(mid) FROM user_basic";

        try (PreparedStatement preparedStatement = connection.prepareStatement(select_max_mid_sql)) {
            var resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                max_mid_value = resultSet.getLong(1);
            }
        }

        // Get max mid value from danmu table
        String select_max_danmu_mid_sql = "SELECT MAX(danmu_id) FROM danmu";

        try (PreparedStatement preparedStatement = connection.prepareStatement(select_max_danmu_mid_sql)) {
            var resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                max_danmu_mid_value = resultSet.getLong(1);
            }
        }

        // Get max mid value from video table
        String select_max_video_mid_sql = "SELECT 1";

        try (PreparedStatement preparedStatement = connection.prepareStatement(select_max_video_mid_sql)) {
            var resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                max_video_mid_value = resultSet.getLong(1);
            }
        }
        // Create mid_max table
        try (PreparedStatement preparedStatement = connection.prepareStatement(create_table_sql)) {
            preparedStatement.executeUpdate();
        }

        // Insert data into mid_max table
        try (PreparedStatement preparedStatement = connection.prepareStatement(insert_data_sql)) {
            preparedStatement.setString(1, "mid_user");
            preparedStatement.setLong(2, max_mid_value);
            preparedStatement.executeUpdate();

            // Insert max_danmu row
            preparedStatement.setString(1, "max_danmu");
            preparedStatement.setLong(2, max_danmu_mid_value);
            preparedStatement.executeUpdate();

            preparedStatement.setString(1, "max_video");
            preparedStatement.setLong(2, max_video_mid_value);
            preparedStatement.executeUpdate();
        }
    }

    private void create_user_table(Connection conn, List<UserRecord> userRecords) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS user_basic (" +
                        "mid BIGINT PRIMARY KEY," +
                        "name VARCHAR," +
                        "sex VARCHAR," +
                        "birthday VARCHAR," +
                        "level INT," +
                        "coin INT," +
                        "sign VARCHAR," +
                        "identity_s  VARCHAR," +
                        "password VARCHAR," +
                        "qq VARCHAR," +
                        "wechat VARCHAR" +
                        ")"
        )) {
            stmt.executeUpdate();

            try (PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO user_basic (mid, name, sex, birthday, level, coin, sign, identity_s, password, qq, wechat) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )) {
                for (UserRecord userRecord : userRecords) {
                    insertStmt.setLong(1, userRecord.getMid());
                    insertStmt.setString(2, userRecord.getName());
                    insertStmt.setString(3, userRecord.getSex());
                    insertStmt.setString(4, userRecord.getBirthday());
                    insertStmt.setInt(5, userRecord.getLevel());
                    insertStmt.setInt(6, userRecord.getCoin());
                    insertStmt.setString(7, userRecord.getSign());
                    insertStmt.setString(8, String.valueOf(userRecord.getIdentity()));
                    insertStmt.setString(9, userRecord.getPassword());
                    insertStmt.setString(10, userRecord.getQq());
                    insertStmt.setString(11, userRecord.getWechat());
                    insertStmt.addBatch();
                }
                insertStmt.executeBatch();
            }
        }
    }

    private void create_and_insert_qq_wechat_tables(Connection conn, List<UserRecord> userRecords) throws SQLException {
        // Create QQ table
        try (PreparedStatement qqTableStmt = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS user_qq (" +
                        "mid BIGINT PRIMARY KEY," +
                        "qq VARCHAR" +
                        ")"
        )) {
            qqTableStmt.executeUpdate();
        }

        // Create WeChat table
        try (PreparedStatement wechatTableStmt = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS user_wechat (" +
                        "mid BIGINT PRIMARY KEY," +
                        "wechat VARCHAR" +
                        ")"
        )) {
            wechatTableStmt.executeUpdate();
        }

        // Insert QQ and WeChat data
        try (PreparedStatement qqStmt = conn.prepareStatement(
                "INSERT INTO user_qq (mid, qq) VALUES (?, ?)"
        );
             PreparedStatement wechatStmt = conn.prepareStatement(
                     "INSERT INTO user_wechat (mid, wechat) VALUES (?, ?)"
             )) {

            for (UserRecord userRecord : userRecords) {
                long mid = userRecord.getMid();
                String qq = userRecord.getQq();
                String wechat = userRecord.getWechat();

                // Insert non-empty QQ
                if (qq != null && !qq.isEmpty()) {
                    qqStmt.setLong(1, mid);
                    qqStmt.setString(2, qq);
                    qqStmt.addBatch();
                }

                // Insert non-empty WeChat
                if (wechat != null && !wechat.isEmpty()) {
                    wechatStmt.setLong(1, mid);
                    wechatStmt.setString(2, wechat);
                    wechatStmt.addBatch();
                }
            }

            qqStmt.executeBatch();
            wechatStmt.executeBatch();
        }
    }

    public void create_viewer_duration_table(Connection conn, List<VideoRecord> videoRecords) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS viewer_duration (" +
                        "bv VARCHAR," +
                        "viewer_mid BIGINT," +
                        "view_time FLOAT," +
                        "PRIMARY KEY (bv, viewer_mid, view_time)" +
                        ")"

        )) {
            stmt.executeUpdate();


            try (PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO viewer_duration (bv, viewer_mid, view_time) VALUES (?, ?, ?)"
            )) {
                for (VideoRecord videoRecord : videoRecords) {
                    String bv = videoRecord.getBv();
                    long[] viewerMids = videoRecord.getViewerMids();
                    float[] viewTimes = videoRecord.getViewTime();

                    for (int i = 0; i < viewerMids.length; i++) {
                        insertStmt.setString(1, bv);
                        insertStmt.setLong(2, viewerMids[i]);
                        insertStmt.setFloat(3, viewTimes[i]);
                        insertStmt.addBatch();
                    }
                }
                insertStmt.executeBatch();
            }
        }
    }

    private void create_following_table(Connection conn, List<UserRecord> userRecords) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS following (" +
                        "user_mid BIGINT," +
                        "following BIGINT," +
                        "CONSTRAINT following_pk PRIMARY KEY (user_mid, following)" +
                        ")"
        )) {
            stmt.executeUpdate();

            try (PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO following (user_mid, following) VALUES (?, ?)"
            )) {
                for (UserRecord userRecord : userRecords) {

                    long userMid = userRecord.getMid();
                    for (Long following : userRecord.getFollowing()) {

                        insertStmt.setLong(1, userMid);
                        insertStmt.setLong(2, following);
                        insertStmt.addBatch();
                    }
                }
                insertStmt.executeBatch();
            }
        }
    }

    private void create_video_table(Connection conn, List<VideoRecord> videoRecords) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS video (" +
                        "bv VARCHAR," +
                        "title VARCHAR," +
                        "owner_mid BIGINT," +
                        "owner_name VARCHAR," +
                        "commit_time TIMESTAMP," +
                        "review_time TIMESTAMP," +
                        "public_time TIMESTAMP," +
                        "duration FLOAT," +
                        "description VARCHAR," +
                        "reviewer BIGINT DEFAULT 0," +
                        "number_of_viewer BIGINT DEFAULT 0," +
                        "CONSTRAINT video_pk PRIMARY KEY (bv)" +
                        ")"
        )) {
            stmt.executeUpdate();

            try (PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO video (bv, title, owner_mid, owner_name, commit_time, review_time, public_time, duration, " +
                            "description, reviewer,number_of_viewer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)"
            )) {
                for (VideoRecord videoRecord : videoRecords) {
                    insertStmt.setString(1, videoRecord.getBv());
                    insertStmt.setString(2, videoRecord.getTitle());
                    insertStmt.setLong(3, videoRecord.getOwnerMid());
                    insertStmt.setString(4, videoRecord.getOwnerName());
                    insertStmt.setTimestamp(5, videoRecord.getCommitTime());
                    insertStmt.setTimestamp(6, videoRecord.getReviewTime());
                    insertStmt.setTimestamp(7, videoRecord.getPublicTime());
                    insertStmt.setFloat(8, videoRecord.getDuration());
                    insertStmt.setString(9, videoRecord.getDescription());
                    insertStmt.setLong(10, videoRecord.getReviewer());
                    insertStmt.setLong(11, videoRecord.getViewerMids().length);
                    insertStmt.addBatch();
                }
                insertStmt.executeBatch();
            }
        }
    }

    private void create_video_like_table(Connection conn, List<VideoRecord> videoRecords) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS video_like (" +
                        "video_bv VARCHAR," +
                        "liked_by BIGINT," +
                        "CONSTRAINT video_like_pk PRIMARY KEY (video_bv, liked_by)" +
                        ")"
        )) {
            stmt.executeUpdate();

            for (VideoRecord videoRecord : videoRecords) {
                try (PreparedStatement insertStmt = conn.prepareStatement(
                        "INSERT INTO video_like (video_bv, liked_by) VALUES (?, ?)"
                )) {
                    String bv = videoRecord.getBv();
                    for (Long likedBy : videoRecord.getLike()) {
                        insertStmt.setString(1, bv);
                        insertStmt.setLong(2, likedBy);
                        insertStmt.addBatch();
                    }
                    insertStmt.executeBatch();
                }
            }
        }
    }

    private void create_video_coin_table(Connection conn, List<VideoRecord> videoRecords) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS video_coin (" +
                        "video_bv VARCHAR," +
                        "coin_by BIGINT," +
                        "CONSTRAINT video_coin_pk PRIMARY KEY (video_bv, coin_by)" +
                        ")"
        )) {
            stmt.executeUpdate();

            try (PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO video_coin (video_bv, coin_by) VALUES (?, ?)"
            )) {
                for (VideoRecord videoRecord : videoRecords) {
                    String bv = videoRecord.getBv();
                    for (Long coinBy : videoRecord.getCoin()) {
                        insertStmt.setString(1, bv);
                        insertStmt.setLong(2, coinBy);
                        insertStmt.addBatch();
                    }
                }
                insertStmt.executeBatch();
            }
        }
    }

    private void create_video_favorite_table(Connection conn, List<VideoRecord> videoRecords) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS video_favorite (" +
                        "video_bv VARCHAR," +
                        "favorite_by BIGINT," +
                        "CONSTRAINT video_favorite_pk PRIMARY KEY (video_bv, favorite_by)" +
                        ")"
        )) {
            stmt.executeUpdate();

            try (PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO video_favorite (video_bv, favorite_by) VALUES (?, ?)"
            )) {
                for (VideoRecord videoRecord : videoRecords) {
                    String bv = videoRecord.getBv();
                    for (Long favoriteBy : videoRecord.getFavorite()) {
                        insertStmt.setString(1, bv);
                        insertStmt.setLong(2, favoriteBy);
                        insertStmt.addBatch();
                    }
                }
                insertStmt.executeBatch();
            }
        }
    }






    @Override
    public void truncate() {
        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
