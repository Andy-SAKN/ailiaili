package io.sustc.service.impl;
import java.security.SecureRandom;
import java.util.*;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {
    private static final String PREFIX = "BV2";
    private static final String CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final int LENGTH = 12;

    private static final SecureRandom random = new SecureRandom();
    private static final Set<String> generatedBVSet = new HashSet<>();

    @Autowired
    private DataSource dataSource;

    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) throws SQLException {
        // Check if the authentication is valid
        try (Connection conn = dataSource.getConnection()) {
            if (!isAuthValid(auth, conn)) {
                return null; // Invalid authentication
            }
            auth.setMid(AuthMidFromQQorWeChat(auth, conn));
        }
        // Check if the request is valid
        if (req == null || req.getTitle() == null || req.getTitle().isEmpty() ||
                req.getDuration() < 10 || req.getPublicTime()==null ) {
            return null; // Invalid request
        }
        if (req.getPublicTime().before(Timestamp.valueOf(LocalDateTime.now())))
            return null;
// Check if there is another video with the same title and same user
        if (isDuplicateVideo(auth.getMid(), req.getTitle())) {
            return null; // Duplicate video
        }
         req.setDuration((long)req.getDuration());
        
        // Insert the video into the database
        return insertVideo(req,auth);
    }



    private boolean isDuplicateVideo(long ownerMid, String title) {
        String sql = "SELECT COUNT(*) FROM video WHERE owner_mid = ? AND title = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1, ownerMid);
            stmt.setString(2, title);

            ResultSet rs = stmt.executeQuery();
            rs.next();

            return rs.getInt(1) > 0; // If count is greater than 0, it's a duplicate
        } catch (SQLException e) {
            throw new RuntimeException("Error checking for duplicate video", e);
        }
    }

    private String insertVideo(PostVideoReq req,AuthInfo auth) {
        String sql = "INSERT INTO Video (bv, title, owner_mid, owner_name, commit_time, review_time, public_time, duration, description) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            // Generate bv (you may use your logic to generate bv)
            String bv = generateBv();

            stmt.setString(1, bv);
            stmt.setString(2, req.getTitle());
            stmt.setLong(3, auth.getMid());
            stmt.setString(4, getPeopleName(auth.getMid()));
            stmt.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now())); // commitTime
            stmt.setTimestamp(6, null); // reviewTime (initially null)
            stmt.setTimestamp(7, req.getPublicTime());
            stmt.setFloat(8, req.getDuration());
            stmt.setString(9, req.getDescription());


            stmt.executeUpdate();

            return bv;
        } catch (SQLException e) {
            throw new RuntimeException("Error inserting video", e);
        }
    }

    public String getPeopleName(long mid) {
        String sql = "SELECT name FROM user_basic WHERE mid = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1, mid);

            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                return rs.getString("name");
            } else {
                return null; // User not found for the given mid
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error retrieving user name", e);
        }
    }


    private String generateBv() {
        String bv;
        do {
            bv = PREFIX + generateRandomString();
        } while (!generatedBVSet.add(bv)); // Ensure the generated BV is not a duplicate

        return bv;
    }

    private String generateRandomString() {
        StringBuilder sb = new StringBuilder(LENGTH);
        for (int i = 0; i < LENGTH; i++) {
            int randomIndex = random.nextInt(CHARACTERS.length());
            char randomChar = CHARACTERS.charAt(randomIndex);
            sb.append(randomChar);
        }
        return sb.toString();
    }












    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {

        // Delete the video and associated records
        try (Connection conn = dataSource.getConnection()) {
            // Check if auth is valid
            if ( !isAuthValid(auth,conn)) {
                return false;
            }
            auth.setMid(AuthMidFromQQorWeChat(auth,conn));
            // Check if the user is the owner or a superuser
            boolean isOwnerOrSuperuser = isOwnerOrSuperuser(auth, bv);
            if (!isOwnerOrSuperuser) {
                return false;
            }

            // Check if the video exists
            if (!videoExists(conn, bv)) {
                return false;
            }

            // Delete records from Video table
            deleteVideoRecord(conn, bv);

            // Delete records from associated tables (likes, favorites, etc.)
            deleteAssociatedRecords(conn, bv);

            return true;
        } catch (SQLException e) {
            throw new RuntimeException("Error deleting video", e);
        }
    }

    private boolean isOwnerOrSuperuser(AuthInfo auth, String bv) {
        // Check if the user is the owner or a superuser
        String sql = "SELECT 1 FROM video WHERE bv = ? AND (owner_mid = ? OR ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, bv);
            stmt.setLong(2, auth.getMid());
            stmt.setBoolean(3, isSuperuser(auth.getMid()));

            ResultSet rs = stmt.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            throw new RuntimeException("Error checking video ownership", e);
        }
    }

    private boolean isSuperuser(long mid) {
        String sql = "SELECT COUNT(*) FROM user_basic WHERE mid = ? AND identity_s = 'SUPERUSER'";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);

            ResultSet rs = stmt.executeQuery();
            rs.next();

            // If the count is greater than 0, the user has SUPERUSER identity
            return rs.getInt(1) > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Error checking SUPERUSER identity", e);
        }
    }


    private boolean videoExists(Connection conn, String bv) throws SQLException {
        String sql = "SELECT 1 FROM video WHERE bv = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);

            ResultSet rs = stmt.executeQuery();
            return rs.next();
        }
    }

    private void deleteVideoRecord(Connection conn, String bv) throws SQLException {
        String sql = "DELETE FROM video WHERE bv = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.executeUpdate();
        }
    }

    private void deleteAssociatedRecords(Connection conn, String bv) throws SQLException {
        // Delete records from VideoLike table
        deleteFromVideoLike(conn, bv);

        // Delete records from VideoFavorite table
        deleteFromVideoFavorite(conn, bv);

        // Delete records from VideoCoin table
        deleteFromVideoCoin(conn, bv);

        // Delete records from viewer_duration table
        deleteFromViewerDuration(conn, bv);

        // Delete records from DanmuLike table
        deleteFromDanmuLike(conn, bv);

        // Delete records from Danmu table
        deleteFromDanmu(conn, bv);

        // Add more delete statements for other associated tables if needed
    }

    private void deleteFromVideoLike(Connection conn, String bv) throws SQLException {
        String sql = "DELETE FROM video_like WHERE video_bv = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.executeUpdate();
        }
    }

    private void deleteFromVideoFavorite(Connection conn, String bv) throws SQLException {
        String sql = "DELETE FROM video_favorite WHERE video_bv = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.executeUpdate();
        }

        // Add more methods for other associated tables if needed
    }

    private void deleteFromVideoCoin(Connection conn, String bv) throws SQLException {
        String sql = "DELETE FROM video_coin WHERE video_bv = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.executeUpdate();
        }
    }

    private void deleteFromViewerDuration(Connection conn, String bv) throws SQLException {
        String sql = "DELETE FROM viewer_duration WHERE bv = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.executeUpdate();
        }
    }

    private void deleteFromDanmuLike(Connection conn, String bv) throws SQLException {
        String sql = "DELETE FROM danmu_like WHERE bv = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.executeUpdate();
        }
    }

    private void deleteFromDanmu(Connection conn, String bv) throws SQLException {
        String sql = "DELETE FROM danmu WHERE bv = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.executeUpdate();
        }
    }

// Add more methods for other associated tables if needed

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        
        try (Connection conn = dataSource.getConnection()) {
            if ( !isAuthValid(auth,conn)) {
                return false;
            }
            auth.setMid(AuthMidFromQQorWeChat(auth,conn));
            if (req == null || req.getTitle() == null || req.getTitle().isEmpty() ||
                    req.getDuration() < 10 || req.getPublicTime()==null ) {
                return false; // Invalid request
            }
            if (req.getPublicTime().before(Timestamp.valueOf(LocalDateTime.now())))
                return false;
// Check if there is another video with the same title and same user
            if (isDuplicateVideo(auth.getMid(), req.getTitle())) {
                return false; // Duplicate video
            }
            if (!isVideoOwner(auth, bv)||!videoExists(conn, bv)) {
                return false; // User is not the owner, return false
            }
            float currentDuration = getCurrentVideoDuration(conn, bv);
            if (req.getDuration() != currentDuration) {
                return false; // Duration is changed, return false
            }
            if (!isVideoInfoChanged(conn, bv, req)) {
                return false; // Video info is not changed, return false
            }
            boolean isReviewerNull = isReviewerNull(conn, bv);
            if (isReviewerNull) {
                return false; // Reviewer column is null, return false
            }

            String updateVideoSql = "UPDATE video SET title = ?, description = ?, public_time = ?, reviewer = ? WHERE bv = ?";
            try (PreparedStatement updateVideoStmt = conn.prepareStatement(updateVideoSql)) {
                updateVideoStmt.setString(1, req.getTitle());
                updateVideoStmt.setString(2, req.getDescription());
                updateVideoStmt.setTimestamp(3, req.getPublicTime());
                updateVideoStmt.setNull(4, Types.BIGINT);
                updateVideoStmt.setString(5, bv);

                int rowsAffected = updateVideoStmt.executeUpdate();
                if (rowsAffected > 0) {
                    return true; // Video info updated successfully
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false; // Error occurred, return false
    }

    @Override
    public synchronized List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        try (Connection conn = dataSource.getConnection()) {
            if (!isAuthValid(auth, conn)) {
                return null;
            }

            auth.setMid(AuthMidFromQQorWeChat(auth, conn));

            if (keywords == null || keywords.isEmpty() || pageSize <= 0 || pageNum <= 0) {
                return null;
            }
            List<String> videoResults = bianLi(conn, keywords, auth);
            List<String> result = new LinkedList<>();
            int startIndex = (pageNum - 1) * pageSize;
            int endIndex = Math.min(startIndex + pageSize, videoResults.size());
            for (int i = startIndex; i < endIndex; i++) {
                String bv = videoResults.get(i).split("\\|")[0];
                result.add(bv);
            }
            conn.close();
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }


    private List<String> bianLi(Connection conn, String keywords, AuthInfo auth) {
        List<String> videoResults = new LinkedList<>();

        // 将关键词拆分成单词
        String[] keywordArray = keywords.split("\\s+");

        // 构建 SQL 查询语句，查询所有视频
        String sql = "SELECT bv, title, description, owner_name, number_of_viewer, reviewer, public_time FROM video";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String bv = rs.getString("bv");
                    String title = rs.getString("title");
                    String description = rs.getString("description");
                    String ownerName = rs.getString("owner_name");
                    long numberOfViewer = rs.getLong("number_of_viewer");

                    // 检查视频是否未审核或未发表状态且不是视频所有者或超级用户
                    Timestamp publicTime = rs.getTimestamp("public_time");
                    long reviewer = rs.getLong("reviewer");
                    boolean isOwnerOrSuperuser = isOwnerOrSuperuser(auth, bv);
                    if ((reviewer == 0 || publicTime == null || publicTime.toLocalDateTime().isAfter(LocalDateTime.now()))&&
                    !isOwnerOrSuperuser) {
                        continue; // 如果是未审核或未发表状态且不是所有者或超级用户，则忽略该视频
                    }
                    // 计算相关性
                    int relevance = calculateRelevance(title, description, ownerName, keywordArray);
                    if(relevance==0)
                        continue;
                    // 将视频 bv 和相关性添加到结果列表中
                    videoResults.add(bv + "|" + relevance + "|"+numberOfViewer);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // 按相关性降序排序
        videoResults.sort((a, b) -> {
            int relevanceA = Integer.parseInt(a.split("\\|")[1]);
            int relevanceB = Integer.parseInt(b.split("\\|")[1]);
            if (relevanceA == relevanceB) {
                // 如果相关性相同，则按观看次数降序排序
                long numberOfViewerA = Integer.parseInt(a.split("\\|")[2]);
                long numberOfViewerB = Integer.parseInt(b.split("\\|")[2]);
                return Long.compare(numberOfViewerB, numberOfViewerA);
            }
            return Integer.compare(relevanceB, relevanceA);
        });

        return videoResults;
    }

    // 计算相关性
    private int calculateRelevance(String title, String description, String ownerName, String[] keywords) {
        int relevance = 0;
        for (String keyword : keywords) {
            keyword = keyword.toLowerCase(); // 忽略大小写
            relevance += countOccurrences(title.toLowerCase(), keyword);
            if (description != null) {
                relevance += countOccurrences(description.toLowerCase(), keyword);
            }
            relevance += countOccurrences(ownerName.toLowerCase(), keyword);
        }
        return relevance;
    }

    // 计算字符串中关键词出现的次数
    private int countOccurrences(String toBeChecked, String keyWord) {
        int count = 0;
        int lastIndex = 0;
        while ((lastIndex = toBeChecked.indexOf(keyWord, lastIndex)) != -1) {
            count++;
            lastIndex += keyWord.length();
        }
        return count;

    }






    private boolean isReviewerNull(Connection conn, String bv) throws SQLException {
        String selectReviewerSql = "SELECT reviewer FROM video WHERE bv = ? AND reviewer IS NULL";
        try (PreparedStatement selectReviewerStmt = conn.prepareStatement(selectReviewerSql)) {
            selectReviewerStmt.setString(1, bv);

            try (ResultSet resultSet = selectReviewerStmt.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private boolean isVideoInfoChanged(Connection conn, String bv, PostVideoReq req) throws SQLException {
        String selectVideoInfoSql = "SELECT title, description, duration, public_time FROM video WHERE bv = ?";
        boolean result = false;

        try (PreparedStatement selectVideoInfoStmt = conn.prepareStatement(selectVideoInfoSql)) {
            selectVideoInfoStmt.setString(1, bv);

            try (ResultSet resultSet = selectVideoInfoStmt.executeQuery()) {
                if (resultSet.next()) {
                    String currentTitle = resultSet.getString("title");
                    String currentDescription = resultSet.getString("description");
                    Float currentDuration = resultSet.getFloat("duration");
                    Timestamp currentPublicTime = resultSet.getTimestamp("public_time");

                    boolean titleChanged = (currentTitle != null && !currentTitle.equals(req.getTitle()))
                            || (currentTitle == null && req.getTitle() != null);

                    boolean descriptionChanged = (currentDescription != null && !currentDescription.equals(req.getDescription()))
                            || (currentDescription == null && req.getDescription() != null);

                    boolean durationChanged = !currentDuration.equals(req.getDuration());

                    boolean publicTimeChanged = (currentPublicTime != null && !currentPublicTime.equals(req.getPublicTime()))
                            || (currentPublicTime == null && req.getPublicTime() != null);
                    result = titleChanged || descriptionChanged || durationChanged || publicTimeChanged;
                }
            }
        }
        return result;
    }


    private float getCurrentVideoDuration(Connection conn, String bv) throws SQLException {
        float currentDuration = -1.0f;

        String selectDurationSql = "SELECT duration FROM video WHERE bv = ?";
        try (PreparedStatement selectDurationStmt = conn.prepareStatement(selectDurationSql)) {
            selectDurationStmt.setString(1, bv);

            try (ResultSet resultSet = selectDurationStmt.executeQuery()) {
                if (resultSet.next()) {
                    currentDuration = resultSet.getFloat("duration");
                }
            }
        }

        return currentDuration;
    }
    private boolean isVideoOwner(AuthInfo auth, String bv) {
        try (Connection conn = dataSource.getConnection()) {
            String checkOwnerSql = "SELECT 1 FROM video WHERE bv = ? AND owner_mid = ?";
            try (PreparedStatement checkOwnerStmt = conn.prepareStatement(checkOwnerSql)) {
                checkOwnerStmt.setString(1, bv);
                checkOwnerStmt.setLong(2, auth.getMid());

                ResultSet resultSet = checkOwnerStmt.executeQuery();
                return resultSet.next(); // If there is a result, user is the owner
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false; // Error occurred, return false
    }









    @Override
    public double getAverageViewRate(String bv) {
        try (Connection conn = dataSource.getConnection()) {
            if (!videoExists(conn, bv) || noPeopleWatched(conn, bv)) {
                return -1; // 视频不存在或者没有人观看，返回 -1
            }
            String avgViewRateQuery = "SELECT AVG(CAST(vd.view_time AS DOUBLE PRECISION) / v.duration) AS avgViewRate " +
                    "FROM viewer_duration vd " +
                    "JOIN video v ON vd.bv = v.bv " +
                    "WHERE v.bv = ?";
            try (PreparedStatement stmt = conn.prepareStatement(avgViewRateQuery)) {
                stmt.setString(1, bv);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    return rs.getDouble("avgViewRate"); // 返回平均观看率
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1; // 出现错误，返回 -1
    }

    private boolean noPeopleWatched(Connection conn, String bv) throws SQLException {
        String query = "SELECT number_of_viewer FROM video WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                int numberOfViewer = rs.getInt("number_of_viewer");
                return numberOfViewer == 0;
            }
        }
        return false; // 查询出错或其他情况，返回 false
    }


    @Override
    public Set<Integer> getHotspot(String bv) {
        Set<Integer> hotspotIndices = new HashSet<>();
        try (Connection conn = dataSource.getConnection()) {
            if (!videoExists(conn, bv) || noDanmu(conn, bv)) {
                return hotspotIndices; // 视频不存在或者没有弹幕，返回空集合
            }

            // 查询视频的所有弹幕数据
            String query = "SELECT time FROM danmu WHERE bv = ? ORDER BY time";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, bv);
                ResultSet rs = stmt.executeQuery();
                List<Float> danmuTimes = new ArrayList<>();
                while (rs.next()) {
                    float time = rs.getFloat("time");
                    danmuTimes.add(time);
                }
                // 计算每个时间段的弹幕数量
                int interval = 10; // 时间段间隔（秒）
                int n = danmuTimes.size();
                int start = 0;
                int end = 0;
                int maxCount = 0;
                List<Integer> maxIndices = new ArrayList<>();
                while (start< n) {
                    float startTime = danmuTimes.get(start);
                    int index = (int) (startTime/10);
                    float endTime = (index+1)*10;
                    while (end < n-1 && danmuTimes.get(end+1) < endTime) {
                        end++;
                    }
                    int count = end - start + 1;
                    if (count > maxCount) {
                        maxCount = count;
                        maxIndices.clear();
                        maxIndices.add(index);
                    } else if (count == maxCount) {
                        maxIndices.add(index);
                    }
                    start = end + 1;
                }

                // 将拥有最多弹幕的时间段索引添加到结果集中
                hotspotIndices.addAll(maxIndices);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return hotspotIndices; // 返回拥有最多弹幕的时间段索引集合
    }

    private boolean noDanmu(Connection conn, String bv) {
        try {
            String query = "SELECT COUNT(*) AS count FROM danmu WHERE bv = ?";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, bv);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    int count = rs.getInt("count");
                    return count == 0;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true; // 出现错误或其他情况，返回 true
    }



    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        try (Connection conn = dataSource.getConnection()) {
            if (!isAuthValid(auth,conn)) {
                return false;
            }
            auth.setMid(AuthMidFromQQorWeChat(auth,conn));
            if(!isSuperuser(auth.getMid())||isVideoOwner(auth,bv)) {
                return false;
            }
            conn.setAutoCommit(false);
            if (!videoExists(conn, bv)) {
                conn.rollback();
                return false;
            }
            if (isVideoReviewed(bv)) {
                conn.rollback();
                return false; // User is not a superuser or the video is already reviewed, return false
            }
            String reviewVideoSql = "UPDATE video SET reviewer =? WHERE bv = ?";
            try (PreparedStatement reviewVideoStmt = conn.prepareStatement(reviewVideoSql)) {
                reviewVideoStmt.setLong(1, auth.getMid());
                reviewVideoStmt.setString(2, bv);

                int rowsAffected = reviewVideoStmt.executeUpdate();
                if (rowsAffected > 0) {
                    conn.commit();
                    return true; // Video reviewed successfully
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false; // Error occurred, return false
    }

    private boolean isVideoReviewed(String bv) {
        if (bv == null || bv.isEmpty()) {
            return false; // Invalid input, return false
        }

        try (Connection conn = dataSource.getConnection()) {
            String checkReviewedSql = "SELECT 1 FROM video WHERE bv = ? AND reviewer <> 0";
            try (PreparedStatement checkReviewedStmt = conn.prepareStatement(checkReviewedSql)) {
                checkReviewedStmt.setString(1, bv);

                ResultSet resultSet = checkReviewedStmt.executeQuery();
                return resultSet.next(); // If there is a result, video is reviewed
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false; // Error occurred, return false
    }


    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        try (Connection conn = dataSource.getConnection()) {
            if (!isAuthValid(auth, conn)) {
                return false; // 用户认证无效，返回 false
            }
            auth.setMid(AuthMidFromQQorWeChat(auth,conn));
            long userId = auth.getMid(); // 获取用户的 mid
            if (!videoExists(conn, bv)) {
                return false;
            }
            // 检查用户是否可以搜索该视频且不是视频的所有者
            if (!canUserSearchVideo(auth, bv, conn) || isVideoOwner(auth, bv)) {
                return false; // 用户不能搜索该视频或者是视频的所有者，返回 false
            }

            // 检查用户是否已经捐赠过硬币给该视频
            if (hasUserDonatedCoin(conn, userId, bv)) {
                return false; // 用户已经捐赠过硬币给该视频，返回 false
            }

            // 检查用户是否有足够的硬币
            if (!hasEnoughCoins(conn, userId)) {
                return false; // 用户硬币数量不足，返回 false
            }


            // 添加一行表示用户对视频的硬币捐赠，并减少用户的硬币数量
            if (donateCoinToVideo(conn, userId, bv)) {
                return true; // 硬币捐赠成功，返回 true
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false; // 出现错误，返回 false
    }

    private boolean hasEnoughCoins(Connection conn, long userId) {
        try {
            String query = "SELECT coin FROM user_basic WHERE mid = ?";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setLong(1, userId);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    int userCoins = rs.getInt("coin");
                    // 检查用户是否拥有足够的硬币
                    return userCoins > 0; // 这里假设用户至少需要拥有一个硬币才能执行操作
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false; // 出现异常或用户不存在，返回 false
    }

    private boolean canUserSearchVideo(AuthInfo auth, String bv, Connection conn) {
        try {
            String query = "SELECT reviewer, public_time, owner_mid FROM video WHERE bv = ?";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, bv);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    long reviewer = rs.getLong("reviewer");
                    Timestamp publicTime = rs.getTimestamp("public_time");
                    long ownerMid = rs.getLong("owner_mid");

                    if ((reviewer == 0 || publicTime.toLocalDateTime().isAfter(LocalDateTime.now())) &&
                            ( ( auth.getMid() != ownerMid) || !isSuperuser(auth.getMid()))) {
                        return false; // 视频未审核或未公开且用户不是超级用户也不是视频所有者，返回 false
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true; // 其它情况都返回 true
    }
    private boolean donateCoinToVideo(Connection conn, long userId, String bv) {
        try {
            // 插入投币记录
            String insertQuery = "INSERT INTO video_coin (video_bv, coin_by) VALUES (?, ?)";
            try (PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {
                insertStmt.setString(1, bv);
                insertStmt.setLong(2, userId);
                int rowsInserted = insertStmt.executeUpdate();

                // 如果成功插入了记录，则更新用户硬币数量
                if (rowsInserted > 0) {
                    // 更新用户硬币数量，假设硬币数量字段为 coin
                    String updateQuery = "UPDATE user_basic SET coin = coin - 1 WHERE mid = ?";
                    try (PreparedStatement updateStmt = conn.prepareStatement(updateQuery)) {
                        updateStmt.setLong(1, userId);
                        int rowsUpdated = updateStmt.executeUpdate();
                        return rowsUpdated > 0; // 返回更新成功与否的结果
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false; // 出现异常或未成功插入投币记录，返回 false
    }

    private boolean hasUserDonatedCoin(Connection conn, long userId, String bv) {
        try {
            String query = "SELECT COUNT(*) AS donation_count FROM video_coin WHERE video_bv = ? AND coin_by = ?";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, bv);
                stmt.setLong(2, userId);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    int donationCount = rs.getInt("donation_count");
                    // 检查用户是否向指定视频捐赠过硬币
                    return donationCount > 0;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false; // 出现异常或用户没有捐赠过硬币，返回 false
    }



    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        try (Connection conn = dataSource.getConnection()) {
            if (!isAuthValid(auth, conn)) {
                return false; // 用户认证无效，返回 false
            }
            auth.setMid(AuthMidFromQQorWeChat(auth,conn));
            if (!videoExists(conn, bv)) {
                return false;
            }
            if (!canUserSearchVideo(auth, bv, conn) || isVideoOwner(auth, bv)) {
                return false; // 用户不能搜索该视频或者是视频的所有者，返回 false
            }

            // Toggle like status
            if (isUserLikedVideo(conn, auth.getMid(), bv)) {
                cancelLikeVideo(conn,auth.getMid(),bv);
                return false;
            } else {
                likeVideo(conn,auth.getMid(),bv);
                return true;
            }
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false; // Error occurred, return false
    }

    private void likeVideo(Connection conn, long mid, String bv) {
        try {
            // 插入点赞记录
            String insertQuery = "INSERT INTO video_like (video_bv, liked_by) VALUES (?, ?)";
            try (PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {
                insertStmt.setString(1, bv);
                insertStmt.setLong(2, mid);
                insertStmt.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void cancelLikeVideo(Connection conn, long mid, String bv) {
        try {
            // 删除点赞记录
            String deleteQuery = "DELETE FROM video_like WHERE video_bv = ? AND liked_by = ?";
            try (PreparedStatement deleteStmt = conn.prepareStatement(deleteQuery)) {
                deleteStmt.setString(1, bv);
                deleteStmt.setLong(2, mid);
                deleteStmt.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private boolean isUserLikedVideo(Connection conn, long mid, String bv) {
        String checkLikeSql = "SELECT 1 FROM video_like WHERE liked_by = ? AND video_bv = ?";
        try (PreparedStatement checkLikeStmt = conn.prepareStatement(checkLikeSql)) {
            checkLikeStmt.setLong(1, mid);
            checkLikeStmt.setString(2, bv);
            ResultSet resultSet = checkLikeStmt.executeQuery();

            return resultSet.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false; // Error occurred, return false
    }


    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        try (Connection conn = dataSource.getConnection()) {
            if (!isAuthValid(auth, conn)) {
                return false; // 用户认证无效，返回 false
            }
            auth.setMid(AuthMidFromQQorWeChat(auth,conn));
            if (!videoExists(conn, bv)) {
                return false;
            }
            if (!canUserSearchVideo(auth, bv, conn) || isVideoOwner(auth, bv)) {
                return false; // 用户不能搜索该视频或者是视频的所有者，返回 false
            }

            // Toggle collect status
            if (isUserCollectVideo(conn, auth.getMid(), bv)) {
                cancelCollectVideo(conn,auth.getMid(),bv);
                return false;
            } else {
                insertCollectVideo(conn,auth.getMid(),bv);
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false; // Error occurred, return false
    }

    private void insertCollectVideo(Connection conn, long mid, String bv) {
        try {
            // 插入收藏视频记录
            String insertQuery = "INSERT INTO video_favorite (video_bv, favorite_by) VALUES (?, ?)";
            try (PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {
                insertStmt.setString(1, bv);
                insertStmt.setLong(2, mid);
                insertStmt.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void cancelCollectVideo(Connection conn, long mid, String bv) {
        try {
            // 取消收藏视频记录
            String deleteQuery = "DELETE FROM video_favorite WHERE video_bv = ? AND favorite_by = ?";
            try (PreparedStatement deleteStmt = conn.prepareStatement(deleteQuery)) {
                deleteStmt.setString(1, bv);
                deleteStmt.setLong(2, mid);
                deleteStmt.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private boolean isUserCollectVideo(Connection conn, long mid, String bv) {
        try {
            // 检查用户是否已经收藏了该视频
            String query = "SELECT COUNT(*) FROM video_favorite WHERE video_bv = ? AND favorite_by = ?";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, bv);
                stmt.setLong(2, mid);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        int count = rs.getInt(1);
                        return count > 0; // 如果结果集中有记录，则表示用户已经收藏了该视频
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false; // 出现异常或者没有找到记录，返回 false
    }


    private void handleSQLException(SQLException e) {
        log.error("SQL State: {}", e.getSQLState());
        log.error("Error Code: {}", e.getErrorCode());
        log.error("Message: {}", e.getMessage());
        log.error("Error Location: {}", e.getStackTrace()[0]);
        throw new RuntimeException("SQL Exception", e);
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
                handleSQLException(e);
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
            handleSQLException(e);
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
}
