package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;
import org.springframework.stereotype.Service;
import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Service
public class RecommenderServiceImpl implements RecommenderService {

    private final DataSource dataSource;

    public RecommenderServiceImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public List<String> recommendNextVideo(String bv) {
        try (Connection conn = dataSource.getConnection()) {
            // 检查视频是否存在
            if (!isVideoExist(conn, bv)) {
                return null;
            }

            // 编写 SQL 查询
            String query = "SELECT v2.bv " +
                    "FROM viewer_duration v1 " +
                    "JOIN viewer_duration v2 ON v1.viewer_mid = v2.viewer_mid " +
                    "JOIN video v ON v1.bv = ? AND v2.bv <> ? AND v.bv = v2.bv " +
                    "GROUP BY v2.bv " +
                    "ORDER BY COUNT(DISTINCT v1.viewer_mid) DESC, v2.bv ASC " +
                    "LIMIT 5";

            List<String> recommendedVideos = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, bv);
                stmt.setString(2, bv);
                ResultSet rs = stmt.executeQuery();
                // 将推荐的视频添加到结果列表中
                while (rs.next()) {

                    recommendedVideos.add(rs.getString("bv"));
                }
            }
            return recommendedVideos;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        if (pageSize <= 0 || pageNum <= 0) {
            return null; // 返回 null，表示参数不合法
        }

        try (Connection conn = dataSource.getConnection()) {
            String query = "SELECT " +
                    "v.bv, " +
                    "CASE " +
                    "    WHEN (SELECT COUNT(DISTINCT viewer_mid) FROM viewer_duration WHERE bv = v.bv) = 0 THEN 0 " +
                    "    ELSE " +
                    "        (SELECT COUNT(1) FROM video_like vl WHERE v.bv = vl.video_bv) / (SELECT COUNT(DISTINCT viewer_mid) FROM viewer_duration WHERE bv = v.bv)::FLOAT + " +
                    "        (SELECT COUNT(1) FROM video_coin vc WHERE v.bv = vc.video_bv) / (SELECT COUNT(DISTINCT viewer_mid) FROM viewer_duration WHERE bv = v.bv)::FLOAT + " +
                    "        (SELECT COUNT(1) FROM video_favorite vf WHERE v.bv = vf.video_bv) / (SELECT COUNT(DISTINCT viewer_mid) FROM viewer_duration WHERE bv = v.bv)::FLOAT + " +
                    "        (SELECT COUNT(1) FROM danmu d WHERE v.bv = d.bv) / (SELECT COUNT(DISTINCT viewer_mid) FROM viewer_duration WHERE bv = v.bv)::FLOAT + " +
                    "        AVG(view_time / duration) " +
                    "END AS total_score " +
                    "FROM " +
                    "video v " +
                    "JOIN " +
                    "viewer_duration vd ON v.bv = vd.bv " +
                    "GROUP BY " +
                    "v.bv " +
                    "ORDER BY " +
                    "total_score DESC, " +
                    "v.bv ASC " +
                    "LIMIT ? OFFSET ?";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setInt(1, pageSize);
                stmt.setInt(2, (pageNum - 1) * pageSize);
                ResultSet rs = stmt.executeQuery();

                List<String> recommendations = new ArrayList<>();
                while (rs.next()) {
                    recommendations.add(rs.getString("bv"));
                }
                return recommendations;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null; // 出现错误，返回 null
    }








    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        List<String> recommendedVideos = new ArrayList<>();
        if (pageSize <= 0 || pageNum <= 0) {
            return null; // 返回 null，表示参数不合法
        }
        try (Connection conn = dataSource.getConnection()) {
            // Check if auth is valid
            if (!isAuthValid(auth, conn)) {
                return null;
            }
            auth.setMid(AuthMidFromQQorWeChat(auth, conn));
            // Get the user's mid
            long userMid = auth.getMid();

            // Check if the user's interest is empty
            if (isUserInterestEmpty(userMid, conn)) {
                // If user's interest is empty, return general recommendations
                return generalRecommendations(pageSize, pageNum);
            }
            // Fetch videos recommended for the user based on their interests
            String sqlQuery = "SELECT v.bv " +
                    "FROM video v " +
                    "JOIN viewer_duration vd ON v.bv = vd.bv " +
                    "WHERE vd.viewer_mid IN (SELECT f1.following " +
                    "FROM following f1 " +
                    "JOIN following f2 ON f1.following = f2.user_mid " +
                    "WHERE f1.user_mid = ? AND f2.following = ?) " +
                    "AND NOT EXISTS (SELECT 1 FROM viewer_duration WHERE viewer_mid = ? AND bv = v.bv) " +
                    "GROUP BY v.bv, v.public_time " +
                    "ORDER BY COUNT(DISTINCT vd.viewer_mid) DESC, " +
                    "(SELECT level FROM user_basic WHERE mid = v.owner_mid) DESC, " +
                    "v.public_time DESC " +
                    "LIMIT ? OFFSET ?";

            try (PreparedStatement pstmt = conn.prepareStatement(sqlQuery)) {
                pstmt.setLong(1, userMid);
                pstmt.setLong(2, userMid);
                pstmt.setLong(3, userMid);
                pstmt.setInt(4, pageSize);
                pstmt.setInt(5, (pageNum - 1) * pageSize);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String bv = rs.getString("bv");
                        recommendedVideos.add(bv);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return recommendedVideos;
    }

    /**
     * Check if the user's interest is empty.
     *
     * @param userMid the user's mid
     * @param conn    the database connection
     * @return true if the user's interest is empty, false otherwise
     * @throws SQLException if a database access error occurs
     */
    private boolean isUserInterestEmpty(long userMid, Connection conn) throws SQLException {
        String friendsQuery = "SELECT f1.following " +
                "FROM following f1 " +
                "JOIN following f2 ON f1.following = f2.user_mid " +
                "WHERE f1.user_mid = ? AND f2.following = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(friendsQuery)) {
            // Assuming pstmt is your PreparedStatement object
            pstmt.setLong(1, userMid);
            pstmt.setLong(2, userMid);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) == 0;
                }
            }
        }
        return true;
    }





    @Override
    public synchronized List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        List<Long> recommendedFriends = new ArrayList<>();
        if (pageSize <= 0 || pageNum <= 0) {
            return null; // 返回 null，表示参数不合法
        }
        try (Connection conn = dataSource.getConnection()) {
            // Check if auth is valid
            if (!isAuthValid(auth, conn)) {
                return null;
            }
            //  System.out.println("原始 "+auth);
            auth.setMid(AuthMidFromQQorWeChat(auth,conn));
            //System.out.println("现在 "+auth);
            // Get the user's mid
            long userMid = auth.getMid();

            // Create temp table for auth's followings
            String createAuthFollowingTableQuery = "CREATE  TABLE IF NOT EXISTS auth_following AS " +
                    "SELECT following " +
                    "FROM following " +
                    "WHERE user_mid = ?";
            try (PreparedStatement createAuthFollowingTableStmt = conn.prepareStatement(createAuthFollowingTableQuery)) {
                createAuthFollowingTableStmt.setLong(1, userMid);
                createAuthFollowingTableStmt.execute();
            }

            // Create temp table for users not followed by the auth user
            String createTempFollowingTableQuery = "CREATE TABLE IF NOT EXISTS temp_following_1 AS " +
                    "SELECT u1.mid AS friend_mid " +
                    "FROM user_basic u1 " +
                    "WHERE u1.mid <> ? AND u1.mid NOT IN (" +
                    "    SELECT f2.following " +
                    "    FROM following f2 " +
                    "    WHERE f2.user_mid = ?" +
                    ")";
            try (PreparedStatement createTempFollowingTableStmt = conn.prepareStatement(createTempFollowingTableQuery)) {
                createTempFollowingTableStmt.setLong(1, userMid);
                createTempFollowingTableStmt.setLong(2, userMid);
                createTempFollowingTableStmt.execute();
            }

            // Fetch recommended friends for the user
            String fetchRecommendationsQuery = "SELECT f1.friend_mid, " +
                    "    (SELECT COUNT(*) " +
                    "     FROM following f2 " +
                    "     WHERE f2.user_mid = f1.friend_mid AND " +
                    "           f2.following IN (SELECT following FROM auth_following) " +
                    "    ) AS common_followings, " +
                    "    (SELECT level FROM user_basic WHERE mid = f1.friend_mid) AS user_level " +
                    "FROM temp_following_1 f1 " +
                    "WHERE EXISTS (" +
                    "    SELECT 1 FROM following f2 " +
                    "    WHERE f2.user_mid = f1.friend_mid AND " +
                    "          f2.following IN (SELECT following FROM auth_following)" +
                    ") " +
                    "ORDER BY common_followings DESC, user_level DESC, f1.friend_mid ASC " +
                    "LIMIT ? OFFSET ?";
            try (PreparedStatement fetchRecommendationsStmt = conn.prepareStatement(fetchRecommendationsQuery)) {
                fetchRecommendationsStmt.setInt(1, pageSize);
                fetchRecommendationsStmt.setInt(2, (pageNum - 1) * pageSize);



                try (ResultSet rs = fetchRecommendationsStmt.executeQuery()) {
                    while (rs.next()) {
                        long recommendedFriendMid = rs.getLong("friend_mid");
                        recommendedFriends.add(recommendedFriendMid);
                    }
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // Clean up: drop temp tables
            try (Connection conn = dataSource.getConnection()) {
                String dropTempTablesQuery = "drop TABLE auth_following; drop TABLE temp_following_1;";
                try (PreparedStatement dropTempTablesStmt = conn.prepareStatement(dropTempTablesQuery)) {
                    dropTempTablesStmt.executeUpdate();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return recommendedFriends;
    }






    private boolean isVideoExist(Connection conn, String bv) throws SQLException {
        String selectSQL = "SELECT 1 FROM video WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(selectSQL)) {
            stmt.setString(1, bv);

            ResultSet rs = stmt.executeQuery();
            return rs.next();  // Return true if a row is found, indicating the video exists
        }
    }

    private boolean isAuthValid(AuthInfo auth,Connection conn) {
        // Check if authentication information is valid
        if (AuthMidFromQQorWeChat(auth,conn)==-1) {
            return false; // Mid is invalid
        }

        if (isNotEmpty(auth.getQq()) && isNotEmpty(auth.getWechat())) {
            // Check if both QQ and WeChat are non-empty
            // and if they correspond to the same user
            try  {
                long userByQq = getUserByQQ(conn, auth.getQq());
                long userByWechat = getUserByWechat(conn, auth.getWechat());

                if ( userByQq != userByWechat) {
                    return false;
                }
            } catch (SQLException e) {
                return false;
            }
        }

        return true;
    }

    private long AuthMidFromQQorWeChat(AuthInfo auth,Connection conn) {
        long authMid = auth.getMid();
        if (isExistInUserBasic(authMid,conn)) {
            return authMid; // Auth has a valid and existing mid
        }

        try  {
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
        }

        return -1; // Default return value
    }
    private boolean isExistInUserBasic(long mid,Connection conn) {
        // Check if user ID is valid by querying the user table
        try {
            String userExistsSql = "SELECT 1 FROM User_basic WHERE mid = ?";
            try (PreparedStatement userExistsStmt = conn.prepareStatement(userExistsSql)) {
                userExistsStmt.setLong(1, mid);
                try (ResultSet userExistsRs = userExistsStmt.executeQuery()) {
                    return userExistsRs.next();
                }
            }
        } catch (SQLException e) {
            return false;
        }
    }

    private boolean isNotEmpty(String value) {
        // Check if a string is not empty
        return value != null && !value.isEmpty()&&!value.equals("null");
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

}
