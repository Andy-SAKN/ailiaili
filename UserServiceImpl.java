package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Service
public class UserServiceImpl implements UserService {

    @Autowired
    private DataSource dataSource;

    @Override
    public long register(RegisterUserReq req) {
        // 检查参数是否有效
        if (req.getPassword() == null || req.getPassword().isEmpty() ||
                req.getName() == null || req.getName().isEmpty() ||
                req.getSex() == null) {
            // 任何必需字段为 null 或空时返回-1
            return -1;
        }

        // 检查生日格式是否有效
        if (req.getBirthday() != null && !isValidBirthdayFormat(req.getBirthday())) {
            return -1;
        }

        // 检查qq和wechat是否已存在
        try (Connection conn = dataSource.getConnection()) {
            if (qqExists(conn, req.getQq()) || wechatExists(conn, req.getWechat())) {
                return -1;
            }
            // 插入User_basic表格
            long mid = insertUserBasic(conn, req);

            // 插入qq和wechat表格
            insertQQWeChat(conn, mid, req.getQq(), req.getWechat());

            return mid;
        } catch (SQLException e) {
            handleSQLException(e);
            throw new RuntimeException("Failed to register user.", e);
        }
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        // Step 1: Check if authentication is valid
        if (!isExistInUserBasic(mid)) {
            return false;
        }
        if (!isAuthValid(auth)) {
            return false;
        }

        auth.setMid(AuthMidFromQQorWeChat(auth));
        try (Connection conn = dataSource.getConnection()) {

            // Step 3: Check the relationship between authenticated user and user to be deleted

            if (!checkRelationship(conn, auth, mid)) {
                return false;
            }
            // Step 4: Delete records associated with the user
            deleteFromDanmu(conn, mid);
            deleteFromDanmuLike(conn, mid);
            deleteFromUserQQ(conn, mid);
            deleteFromUserWeChat(conn, mid);
            deleteFromViewerDuration(conn, mid);
            deleteFromFollowing(conn, mid);
            deleteFromVideo(conn, mid);
            deleteFromVideoLike(conn, mid);
            deleteFromVideoCoin(conn, mid);
            deleteFromVideoFavorite(conn, mid);
            deleteFromFollower(conn,mid);
            // Add more delete operations as needed...

            // Step 5: Delete the user record
            deleteUser(conn, mid);

            return true;
        } catch (SQLException e) {
            // Handle SQLException
            e.printStackTrace();
            return false;
        }
    }
    private boolean checkRelationship(Connection conn, AuthInfo auth, long mid) throws SQLException {
        String selectSQL = "SELECT identity_s FROM user_basic WHERE mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(selectSQL)) {
            stmt.setLong(1, auth.getMid());

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                String identity = rs.getString("identity_s");

                // Define identity values
                String superuser = "SUPERUSER";
                String regularuser = "USER";

                // Check the relationship based on identity
                if (superuser.equals(identity)) {
                    // The authenticated user is a superuser
                    if (auth.getMid() == mid) {
                        // Superuser can delete their own account
                        return true;
                    } else {
                        // Superuser cannot delete other superusers
                        return regularuser.equals(getIdentity(conn, mid));
                    }
                }else if (regularuser.equals(identity)) {
                    // The authenticated user is a regular user, can only delete own account
                    return auth.getMid() == mid;
                }
            }
        }

        // Default to false if the relationship check fails
        return false;
    }
    private String getIdentity(Connection conn, long mid) throws SQLException {
        String selectSQL = "SELECT identity_s FROM user_basic WHERE mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(selectSQL)) {
            stmt.setLong(1, mid);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getString("identity_s");
            }
        }

        // Return null if the identity is not found
        return null;
    }

    private void deleteFromDanmu(Connection conn, long mid) throws SQLException {
        deleteFromTable(conn, "danmu", "mid", mid);
    }

    private void deleteFromDanmuLike(Connection conn, long mid) throws SQLException {
        deleteFromTable(conn, "danmu_like", "danmu_id", mid);
    }

    private void deleteFromUserQQ(Connection conn, long mid) throws SQLException {
        deleteFromTable(conn, "user_qq", "mid", mid);
    }

    private void deleteFromUserWeChat(Connection conn, long mid) throws SQLException {
        deleteFromTable(conn, "user_wechat", "mid", mid);
    }

    private void deleteFromViewerDuration(Connection conn, long mid) throws SQLException {
        deleteFromTable(conn, "viewer_duration", "viewer_mid", mid);
    }

    private void deleteFromFollowing(Connection conn, long mid) throws SQLException {
        deleteFromTable(conn, "following", "user_mid", mid);
    }
    private void deleteFromFollower(Connection conn, long mid) throws SQLException {
        deleteFromTable(conn, "following", "following", mid);
    }

    private void deleteFromVideo(Connection conn, long mid) throws SQLException {
        deleteFromTable(conn, "video", "owner_mid", mid);
    }

    private void deleteFromVideoLike(Connection conn, long mid) throws SQLException {
        deleteFromTable(conn, "video_like", "liked_by", mid);
    }

    private void deleteFromVideoCoin(Connection conn, long mid) throws SQLException {
        deleteFromTable(conn, "video_coin", "coin_by", mid);
    }

    private void deleteFromVideoFavorite(Connection conn, long mid) throws SQLException {
        deleteFromTable(conn, "video_favorite", "favorite_by", mid);
    }

    // Add more methods as needed...

    private void deleteUser(Connection conn, long mid) throws SQLException {
        String deleteSQL = "DELETE FROM user_basic WHERE mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(deleteSQL)) {
            stmt.setLong(1, mid);
            stmt.executeUpdate();
        }
    }

    // Utility methods...

    private void deleteFromTable(Connection conn, String tableName, String columnName, long mid) throws SQLException {
        String deleteSQL = "DELETE FROM " + tableName + " WHERE " + columnName + " = ?";
        try (PreparedStatement stmt = conn.prepareStatement(deleteSQL)) {
            stmt.setLong(1, mid);
            stmt.executeUpdate();
        }
    }

    // Add more utility methods as needed...

    @Override
    public UserInfoResp getUserInfo(long mid) {
        try (Connection conn = dataSource.getConnection()) {
            UserInfoResp userInfoResp = new UserInfoResp();
            userInfoResp.setMid(mid);

            // Check if user exists in user_basic table
            if (!isUserExists(conn, mid)) {
                return null;  // User does not exist
            }

            // Get user's coin from user_basic table
            userInfoResp.setCoin(getUserCoin(conn, mid));

            // Get user's following and follower lists
            userInfoResp.setFollowing(getFollowing(conn, mid));
            userInfoResp.setFollower(getFollower(conn, mid));

            // Get user's watched, liked, collected, and posted videos
            userInfoResp.setWatched(getWatchedVideos(conn, mid));
            userInfoResp.setLiked(getLikedVideos(conn, mid));
            userInfoResp.setCollected(getCollectedVideos(conn, mid));
            userInfoResp.setPosted(getPostedVideos(conn, mid));

            return userInfoResp;
        } catch (SQLException e) {
            handleSQLException(e);
            return null;
        }
    }



    private boolean isValidBirthdayFormat(String birthday) {
        String regex = "^(0?[1-9]|1[0-2])月(0?[1-9]|[1-2][0-9]|3[0-1])日$";
        if (!birthday.matches(regex)) {
            return false;
        }

        String[] parts = birthday.split("月");
        int month = Integer.parseInt(parts[0]);
        int day = Integer.parseInt(parts[1].substring(0, parts[1].length() - 1));

        return isValidMonthDay(month, day);
    }

    private boolean isValidMonthDay(int month, int day) {
        if (month >= 1 && month <= 12) {
            if ((month == 1 || month == 3 || month == 5 || month == 7 || month == 8 || month == 10 || month == 12) && (day >= 1 && day <= 31)) {
                return true;
            } else if ((month == 4 || month == 6 || month == 9 || month == 11) && (day >= 1 && day <= 30)) {
                return true;
            } else return month == 2 && (day >= 1 && day <= 29);
        }
        return false;
    }
    private boolean isUserExists(Connection conn, long mid) throws SQLException {
        // Check if user exists in user_basic table
        String userExistsSql = "SELECT 1 FROM user_basic WHERE mid = ?";
        try (PreparedStatement userExistsStmt = conn.prepareStatement(userExistsSql)) {
            userExistsStmt.setLong(1, mid);
            try (ResultSet userExistsRs = userExistsStmt.executeQuery()) {
                return userExistsRs.next();
            }
        }
    }

    private int getUserCoin(Connection conn, long mid) throws SQLException {
        // Get user's coin from user_basic table
        String userCoinSql = "SELECT coin FROM user_basic WHERE mid = ?";
        try (PreparedStatement userCoinStmt = conn.prepareStatement(userCoinSql)) {
            userCoinStmt.setLong(1, mid);
            try (ResultSet userCoinRs = userCoinStmt.executeQuery()) {
                return userCoinRs.next() ? userCoinRs.getInt("coin") : 0;
            }
        }
    }
    private long[] getFollowing(Connection conn, long mid) throws SQLException {
        // 查询 Following 表获取关注列表
        String followingSql = "SELECT following FROM following WHERE user_mid = ?";
        return getLongArray(conn, followingSql, mid);
    }

    private long[] getFollower(Connection conn, long mid) throws SQLException {
        // 查询 Following 表获取粉丝列表
        String followerSql = "SELECT user_mid FROM following WHERE following = ?";
        return getLongArray(conn, followerSql, mid);
    }

    private String[] getWatchedVideos(Connection conn, long mid) throws SQLException {
        // 查询 VideoViewerMids 表获取观看视频列表
        String watchedSql = "SELECT distinct bv FROM viewer_duration WHERE viewer_mid = ?";
        return getStringArray(conn, watchedSql, mid);
    }

    private String[] getLikedVideos(Connection conn, long mid) throws SQLException {
        // 查询 VideoLike 表获取喜欢视频列表
        String likedSql = "SELECT video_bv FROM video_like WHERE liked_by = ?";
        return getStringArray(conn, likedSql, mid);
    }

    private String[] getCollectedVideos(Connection conn, long mid) throws SQLException {
        // 查询 VideoFavorite 表获取收藏视频列表
        String collectedSql = "SELECT video_bv FROM video_favorite WHERE favorite_by = ?";
        return getStringArray(conn, collectedSql, mid);
    }

    private String[] getPostedVideos(Connection conn, long mid) throws SQLException {
        // 查询 Video 表获取发布视频列表
        String postedSql = "SELECT bv FROM Video WHERE owner_mid = ?";
        return getStringArray(conn, postedSql, mid);
    }

    private long[] getLongArray(Connection conn, String sql, long mid) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);
            try (ResultSet rs = stmt.executeQuery()) {
                return resultSetToLongArray(rs);
            }
        }
    }

    private String[] getStringArray(Connection conn, String sql, long mid) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);
            try (ResultSet rs = stmt.executeQuery()) {
                return resultSetToStringArray(rs);
            }
        }
    }

    private long[] resultSetToLongArray(ResultSet rs) throws SQLException {
        List<Long> list = new ArrayList<>();
        while (rs.next()) {
            list.add(rs.getLong(1));
        }
        return list.stream().mapToLong(Long::longValue).toArray();
    }

    private String[] resultSetToStringArray(ResultSet rs) throws SQLException {
        List<String> list = new ArrayList<>();
        while (rs.next()) {
            list.add(rs.getString(1));
        }
        return list.toArray(new String[0]);
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

    // ... 其他方法


    private boolean qqExists(Connection conn, String qq) {
        String sql = "SELECT COUNT(*) FROM user_qq WHERE qq = ?";
        return exists(conn, sql, qq);
    }

    private boolean wechatExists(Connection conn, String wechat) {
        String sql = "SELECT COUNT(*) FROM user_wechat WHERE wechat = ?";
        return exists(conn, sql, wechat);
    }

    private boolean exists(Connection conn, String sql, String value) {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, value);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            handleSQLException(e);
            return false;
        }
    }

    private long insertUserBasic(Connection conn, RegisterUserReq req) throws SQLException {
        String sql = "INSERT INTO User_basic (mid, name, sex, birthday, level, coin, sign, identity_s, password, qq, wechat) " +
                "VALUES (?, ?, ?, ?, 0, 0, '', 'USER', ?, ?, ?)";

        try (PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            stmt.setLong(1, generateMid(conn));
            stmt.setString(2, req.getName());
            stmt.setString(3, req.getSex().name());
            stmt.setString(4, req.getBirthday());
            stmt.setString(5, req.getPassword());
            stmt.setString(6, req.getQq());
            stmt.setString(7, req.getWechat());

            stmt.executeUpdate();

            ResultSet generatedKeys = stmt.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getLong(1);
            } else {
                throw new SQLException("Failed to get generated key.");
            }
        }
    }

    private void insertQQWeChat(Connection conn, long mid, String qq, String wechat) throws SQLException {
        if (qq != null && !qq.isEmpty()) {
            String qqSql = "INSERT INTO user_qq (mid, qq) VALUES (?, ?)";
            try (PreparedStatement qqStmt = conn.prepareStatement(qqSql)) {
                qqStmt.setLong(1, mid);
                qqStmt.setString(2, qq);
                qqStmt.executeUpdate();
            }
        }

        if (wechat != null && !wechat.isEmpty()) {
            String wechatSql = "INSERT INTO user_wechat (mid, wechat) VALUES (?, ?)";
            try (PreparedStatement wechatStmt = conn.prepareStatement(wechatSql)) {
                wechatStmt.setLong(1, mid);
                wechatStmt.setString(2, wechat);
                wechatStmt.executeUpdate();
            }
        }
    }

    private long generateMid(Connection conn) throws SQLException {
        long currentMaxMid = getCurrentMaxMid(conn);
        long newMid = currentMaxMid + 1;
        updateMidMax(conn, newMid);
        return newMid;
    }

    private long getCurrentMaxMid(Connection conn) throws SQLException {
        String sql = "SELECT mid_value FROM mid_max WHERE mid_name = 'mid_user'";
        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getLong("mid_value");
            } else {
                throw new SQLException("Failed to get current max mid from mid_max.");
            }
        }
    }

    private void updateMidMax(Connection conn, long newMid) throws SQLException {
        String sql = "UPDATE mid_max SET mid_value = ? WHERE mid_name = 'mid_user'";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, newMid);
            int rowsUpdated = stmt.executeUpdate();
            if (rowsUpdated == 0) {
                throw new SQLException("Failed to update mid_max with new mid.");
            }
        }
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeMid) {
        // Check if authentication information is valid


        if (!isAuthValid(auth)) {
            return false;
        }

        // Check if followeeMid is valid
        if (!isExistInUserBasic(followeeMid)) {
            return false;
        }
        auth.setMid(AuthMidFromQQorWeChat(auth));
        if(auth.getMid()==followeeMid){
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            // Implement follow/unfollow logic here
            boolean isFollowing = isFollowing(conn, auth, followeeMid);

            if (isFollowing) {
                // User is already following, unfollow the user
                unfollowUser(conn, auth, followeeMid);
                return false;  // Unfollowed
            } else {
                // User is not following, follow the user
                followUser(conn, auth, followeeMid);
                return true;  // Followed
            }
        } catch (SQLException e) {
            handleSQLException(e);
            return false;
        }
    }

    // Additional helper methods for follow/unfollow logic
    private boolean isFollowing(Connection conn, AuthInfo auth, long followeeMid) throws SQLException {
        String isFollowingSql = "SELECT 1 FROM following WHERE user_mid = ? AND following = ?";
        try (PreparedStatement isFollowingStmt = conn.prepareStatement(isFollowingSql)) {
            isFollowingStmt.setLong(1, auth.getMid());
            isFollowingStmt.setLong(2, followeeMid);
            try (ResultSet isFollowingRs = isFollowingStmt.executeQuery()) {
                return isFollowingRs.next();
            }
        }
    }

    private void followUser(Connection conn, AuthInfo auth, long followeeMid) throws SQLException {
        String followUserSql = "INSERT INTO following (user_mid, following) VALUES (?, ?)";
        try (PreparedStatement followUserStmt = conn.prepareStatement(followUserSql)) {
            followUserStmt.setLong(1, auth.getMid());
            followUserStmt.setLong(2, followeeMid);
            followUserStmt.executeUpdate();
        }
    }

    private void unfollowUser(Connection conn, AuthInfo auth, long followeeMid) throws SQLException {
        String unfollowUserSql = "DELETE FROM following WHERE user_mid = ? AND following = ?";
        try (PreparedStatement unfollowUserStmt = conn.prepareStatement(unfollowUserSql)) {
            unfollowUserStmt.setLong(1, auth.getMid());
            unfollowUserStmt.setLong(2, followeeMid);
            unfollowUserStmt.executeUpdate();
        }
    }
    // ... Other methods from the original class

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
            String userExistsSql = "SELECT 1 FROM user_basic WHERE mid = ?";
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

// ... Other methods from the original class

}