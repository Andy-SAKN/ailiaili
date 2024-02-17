package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.DanmuRecord;
import io.sustc.service.DanmuService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.sql.*;



@Service
public class DanmuServiceImpl implements DanmuService {
    static int test = 888;
    @Autowired
    private DataSource dataSource;
    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        // Check if authentication is valid
        if (!isAuthValid(auth)) {
            return -1;
        }
        auth.setMid(AuthMidFromQQorWeChat(auth));

        try (Connection conn = dataSource.getConnection()) {
            // Check if the video exists and if the user has watched it
            if(!isVideoExist(conn,bv))
                return -1;
            if (content == null || content.isEmpty()) {
                return -1;  // Invalid content
            }
            if (!hasUserWatchedVideo(conn, auth.getMid(), bv)) {
                return -1;  // Video not found or user has not watched the video
            }
            if(!isPublished(conn,bv))
                return -1;
            // Check if the content is valid
            if(timeWrong(bv,time,conn))
                return -1;
            // Insert the danmu into the Danmu table
            return insertDanmu(conn, auth, bv, content, time);
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;  // Exception occurred
        }
    }

    private boolean timeWrong(String bv, float time, Connection conn) {
        String selectDurationSQL = "SELECT duration FROM video WHERE bv = ?";

        try (PreparedStatement preparedStatement = conn.prepareStatement(selectDurationSQL)) {
            preparedStatement.setString(1, bv);
            ResultSet resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                float duration = resultSet.getFloat("duration");

                // Check if duration is smaller than time or time is less than 0
                return duration < time ;
            }
        } catch (SQLException e) {
            // Handle SQLException
            e.printStackTrace();
        }

        // Return false if no duration information is found
        return false;
    }


    private boolean isVideoExist(Connection conn, String bv) throws SQLException {
        String selectSQL = "SELECT 1 FROM Video WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(selectSQL)) {
            stmt.setString(1, bv);

            ResultSet rs = stmt.executeQuery();
            return rs.next();  // Return true if a row is found, indicating the video exists
        }
    }


    private boolean isPublished(Connection conn, String bv) throws SQLException {
        String selectSQL = "SELECT public_time FROM Video WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(selectSQL)) {
            stmt.setString(1, bv);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                Timestamp publicTime = rs.getTimestamp("public_time");
                return publicTime != null;
            } else {
                return false;  // Video not found
            }
        }
    }


    private boolean hasUserWatchedVideo(Connection conn, long mid, String bv) throws SQLException {
        String selectSQL = "SELECT 1 FROM viewer_duration WHERE viewer_mid = ? AND bv = ? AND view_time >=0";
        try (PreparedStatement stmt = conn.prepareStatement(selectSQL)) {
            stmt.setLong(1, mid);
            stmt.setString(2, bv);

            ResultSet rs = stmt.executeQuery();
            return rs.next();
        }
    }

    private long insertDanmu(Connection conn, AuthInfo auth, String bv, String content, float time) throws SQLException {
        String insertSQL = "INSERT INTO Danmu (bv, mid, time, content, post_time, test) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, lastval())";

        try (PreparedStatement stmt = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)) {
            stmt.setString(1, bv);
            stmt.setLong(2, auth.getMid());
            stmt.setFloat(3, time);
            stmt.setString(4, content);

            int affectedRows = stmt.executeUpdate();

            if (affectedRows == 0) {
                // Handle the case when no rows are inserted
                throw new SQLException("Inserting Danmu failed, no rows affected.");
            }

            // Retrieve the last inserted ID using getGeneratedKeys
            try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                } else {
                    throw new SQLException("Failed to retrieve last inserted ID.");
                }
            }
        }
    }








    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
        if (!isPublished(bv) || timeStart > timeEnd || timeStart < 0 || timeEnd < 0) {
            return null;
        }

        String selectDanmuSQL;
        if (filter) {
            // 使用 GROUP BY content，并选择每个组中最早发布的弹幕
            selectDanmuSQL = "SELECT danmu_id FROM danmu WHERE bv = ? AND time >= ? AND time <= ? " +
                    "AND (content, time) IN (SELECT content, MIN(time) FROM danmu WHERE bv = ? AND time >= ? AND time <= ? GROUP BY content)";
        } else {
            selectDanmuSQL = "SELECT danmu_id FROM danmu WHERE bv = ? AND time >= ? AND time <= ?";
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(selectDanmuSQL)) {
            if (timeWrong(bv, timeEnd, conn) || !isVideoExist(conn, bv)) {
                return null;
            }

            preparedStatement.setString(1, bv);
            preparedStatement.setFloat(2, timeStart);
            preparedStatement.setFloat(3, timeEnd);

            // 如果是 filter 模式，需要再次设置 bv、timeStart 和 timeEnd 的值
            if (filter) {
                preparedStatement.setString(4, bv);
                preparedStatement.setFloat(5, timeStart);
                preparedStatement.setFloat(6, timeEnd);
            }

            ResultSet resultSet = preparedStatement.executeQuery();
            List<Long> danmuIds = new ArrayList<>();
            while (resultSet.next()) {
                danmuIds.add(resultSet.getLong(1));
            }

            // 对返回的弹幕 id 按照它们对应的 time 进行排序
            danmuIds.sort(Comparator.comparingLong(this::getDanmuTime));

            return danmuIds;

        } catch (SQLException e) {
            // 处理异常
            e.printStackTrace();
            return null;
        }
    }

    // 获取弹幕的时间
    private long getDanmuTime(long danmuId) {
        String queryTimeSQL = "SELECT time FROM danmu WHERE danmu_id = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(queryTimeSQL)) {

            preparedStatement.setLong(1, danmuId);

            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getLong("time");
            } else {
                // 如果没有找到对应的弹幕，可以返回一个默认值或者抛出异常
                throw new RuntimeException("No corresponding danmu found for danmu_id: " + danmuId);
            }


        } catch (SQLException e) {
            // 处理异常
            e.printStackTrace();
            // 如果查询过程中发生异常，也可以返回默认值或者抛出异常
            throw new RuntimeException("Error while querying danmu time for danmu_id: " + danmuId, e);
        }
    }



    private boolean isPublished(String bv) {
        String selectPublishedSQL = "SELECT public_time FROM Video WHERE bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(selectPublishedSQL)) {

            preparedStatement.setString(1, bv);

            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet.next() && resultSet.getTimestamp("public_time") != null;

        } catch (SQLException e) {
            // 处理异常
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {
        if (!isAuthValid(auth)) {
            return false;
        }
        auth.setMid(AuthMidFromQQorWeChat(auth));

        try (Connection conn = dataSource.getConnection()) {
            if(!isDanmuExist(id,conn)) {
                return false;
            }


            // Check if the user has already liked the danmu
            boolean alreadyLiked = hasUserLikedDanmu(conn, auth.getMid(), id);

            if (alreadyLiked) {
                // User has already liked the danmu, cancel the like status
                cancelDanmuLike(conn, auth.getMid(), id);
            } else {
                // User hasn't liked the danmu, like it
                if(hasWatched(auth,id,conn))
                    likeDanmu(conn, auth.getMid(), id);
                else
                    alreadyLiked = !alreadyLiked;
            }
            return !alreadyLiked;

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean hasWatched(AuthInfo auth, long id, Connection conn) {
        try {
            String selectSQL = "SELECT 1 FROM viewer_duration " +
                    "WHERE viewer_mid = ? AND bv = ? AND view_time >= 0";
            try (PreparedStatement stmt = conn.prepareStatement(selectSQL)) {
                stmt.setLong(1, auth.getMid());
                stmt.setString(2, getDanmuBVById(conn, id));

                ResultSet rs = stmt.executeQuery();
                return rs.next();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    private String getDanmuBVById(Connection conn, long id) throws SQLException {
        String selectSQL = "SELECT bv FROM danmu WHERE danmu_id = ?";
        try (PreparedStatement stmt = conn.prepareStatement(selectSQL)) {
            stmt.setLong(1, id);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getString("bv");
            }
        }
        return null;
    }


    private boolean hasUserLikedDanmu(Connection conn, long mid, long danmuId) throws SQLException {
        String selectSQL = "SELECT 1 FROM danmu_like WHERE danmu_id = ? AND liked_by = ?";
        try (PreparedStatement stmt = conn.prepareStatement(selectSQL)) {
            stmt.setLong(1, danmuId);
            stmt.setLong(2, mid);

            ResultSet rs = stmt.executeQuery();
            return rs.next();
        }
    }

    private void likeDanmu(Connection conn, long mid, long danmuId) throws SQLException {
        String insertSQL = "INSERT INTO danmu_like (danmu_id, liked_by,bv) VALUES (?, ?,?)";
        try (PreparedStatement stmt = conn.prepareStatement(insertSQL)) {
            stmt.setLong(1, danmuId);
            stmt.setLong(2, mid);
            stmt.setString(3, getDanmuBVById(conn,danmuId));
            stmt.executeUpdate();
        }
    }

    private void cancelDanmuLike(Connection conn, long mid, long danmuId) throws SQLException {
        String deleteSQL = "DELETE FROM danmu_like WHERE danmu_id = ? AND liked_by = ?";
        try (PreparedStatement stmt = conn.prepareStatement(deleteSQL)) {
            stmt.setLong(1, danmuId);
            stmt.setLong(2, mid);
            stmt.executeUpdate();
        }
    }

    private boolean isAuthValid(AuthInfo auth) {
        // Check if authentication information is valid
        if (AuthMidFromQQorWeChat(auth)==-1) {
            return false; // Mid is invalid
        }

        if (isNotEmpty(auth.getQq()) && isNotEmpty(auth.getWechat())) {
            // Check if both QQ and WeChat are non-empty
            // and if they correspond to the same user
            try (Connection conn = dataSource.getConnection()) {
                long userByQq = getUserByQQ(conn, auth.getQq());
                long userByWechat = getUserByWechat(conn, auth.getWechat());

                if ( userByQq != userByWechat) {
                    return false;
                }
            } catch (SQLException e) {
                handleSQLException(e);
                return false;
            }
        }

        return true;
    }

    private long AuthMidFromQQorWeChat(AuthInfo auth) {
        long authMid = auth.getMid();
        if (isExistInUserBasic(authMid)) {
            return authMid; // Auth has a valid and existing mid
        }

        try (Connection conn = dataSource.getConnection()) {
            // If auth's mid does not exist, check by QQ
            if (isNotEmpty(auth.getQq())) {
                long qqMid = getUserByQQ(conn, auth.getQq());
                if (qqMid > 0) {
                    return qqMid; // User found by QQ
                }
            }

            // If auth's mid and QQ do not exist, check by WeChat
            if (isNotEmpty(auth.getWechat())) {
                long wechatMid = getUserByWechat(conn, auth.getWechat());
                if (wechatMid > 0) {
                    return wechatMid; // User found by WeChat
                }
            }
        } catch (SQLException e) {
            handleSQLException(e);
        }

        return -1; // Default return value
    }

    private boolean isExistInUserBasic(long mid) {
        // Check if user ID is valid by querying the user table
        try (Connection conn = dataSource.getConnection()) {
            String userExistsSql = "SELECT 1 FROM User_basic WHERE mid = ?";
            try (PreparedStatement userExistsStmt = conn.prepareStatement(userExistsSql)) {
                userExistsStmt.setLong(1, mid);
                try (ResultSet userExistsRs = userExistsStmt.executeQuery()) {
                    return userExistsRs.next();
                }
            }
        } catch (SQLException e) {
            handleSQLException(e);
            return false;
        }
    }

    private boolean isNotEmpty(String value) {
        // Check if a string is not empty
        return value != null && !value.isEmpty();
    }
    private void handleSQLException(SQLException e) {
        System.err.println("SQL Exception:");
        while (e != null) {
            System.err.println("Message: " + e.getMessage());
            System.err.println("SQL State: " + e.getSQLState());
            System.err.println("Error Code: " + e.getErrorCode());
            e = e.getNextException();
        }
    }
    private long getUserByQQ(Connection conn, String qq) throws SQLException {
        String sql = "SELECT mid FROM user_qq WHERE qq = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, qq);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? rs.getLong("mid") : -1;
            }
        }
    }

    private long getUserByWechat(Connection conn, String wechat) throws SQLException {
        String sql = "SELECT mid FROM user_wechat WHERE wechat = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, wechat);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? rs.getLong("mid") : -1;
            }
        }
    }
    public boolean isDanmuExist(long id, Connection conn) {
        try {
            String selectSQL = "SELECT 1 FROM danmu WHERE danmu_id = ?";
            try (PreparedStatement stmt = conn.prepareStatement(selectSQL)) {
                stmt.setLong(1, id);

                ResultSet rs = stmt.executeQuery();
                return rs.next();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }
}
