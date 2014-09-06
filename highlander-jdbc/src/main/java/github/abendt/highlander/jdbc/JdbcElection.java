package github.abendt.highlander.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

public class JdbcElection {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcElection.class);

    int maxHeartBeatAge = 1000 * 10;
    Connection connection;
    String groupName;
    String participantId = UUID.randomUUID().toString();

    public JdbcElection(String groupName) {
        this.groupName = groupName;
    }

    public void setMaxHeartBeatAge(int maxHeartBeatAge) {
        this.maxHeartBeatAge = maxHeartBeatAge;
    }

    public void giveUpLeaderShip(Connection connection) {
        this.connection = connection;

        try {
            Election election = findElection(groupName);

            while (true) {
                if (!election.isParticipantCurrentGroupLeader(participantId)) {
                    return;
                }

                election = updateGroupLeader(election, null);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    private class Election {
        String electionGroupName;
        String electedParticipant;
        Date lastHeartBeat;
        int version;

        boolean isParticipantCurrentGroupLeader(String participantId) {
            return participantId.equals(electedParticipant);
        }

        boolean hasNoCurrentGroupLeader() {
            return electedParticipant == null;
        }

        boolean isHeartBeatTooOld() {
            if (lastHeartBeat == null) {
                return true;
            }

            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.MILLISECOND, -maxHeartBeatAge);

            return cal.getTime().after(lastHeartBeat);
        }

    }

    public boolean runElection(Connection connection) {
        this.connection = connection;

        LOG.debug("participant '{}' begins election for group '{}'", participantId, groupName);

        try {
            Election election = loadOrInsertElection(groupName, participantId);

            while (true) {
                if (election.hasNoCurrentGroupLeader()) {
                    LOG.debug("group '{}' has no leader. participant '{}' requests leadership", groupName, participantId);
                    election = updateGroupLeader(election, participantId);
                    continue;
                }

                if (election.isHeartBeatTooOld()) {
                    LOG.debug("group '{}': heartbeat '{}' is too old. participant '{}' requests leadership", groupName, election.lastHeartBeat, participantId);
                    election = updateGroupLeader(election, participantId);
                    continue;
                }

                if (election.isParticipantCurrentGroupLeader(participantId)) {
                    election = updateGroupLeader(election, participantId);
                }

                LOG.debug("participant '{}' is the leader of '{}'", election.electedParticipant, groupName);
                return election.isParticipantCurrentGroupLeader(participantId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Election updateGroupLeader(Election election, String participantId) throws SQLException {
        election.electedParticipant = participantId;
        return updateElection(election);
    }

    private Election updateElection(Election election) throws SQLException {

        StringBuilder sql = new StringBuilder("UPDATE HIGHLANDER SET ");

        if (election.electedParticipant != null) {
            sql.append("ID=?, ");
        } else {
            sql.append("ID=NULL, ");
        }

        sql.append("HEARTBEAT=?, VERSION=? WHERE GROUPNAME=? AND VERSION=?");

        int paramIndex = 1;

        PreparedStatement st = connection.prepareStatement(sql.toString());

        try {
            int incrementedVersion = election.version + 1;

            if (election.electedParticipant != null) {
                st.setString(paramIndex++, participantId);
            }

            st.setTimestamp(paramIndex++, timestamp(new Date()));
            st.setInt(paramIndex++, incrementedVersion);
            st.setString(paramIndex++, election.electionGroupName);
            st.setInt(paramIndex++, election.version);

            st.execute();

            return findElection(election.electionGroupName);
        } finally {
            st.close();
        }
    }

    private Election findElection(final String groupName) throws SQLException {
        Statement statement = connection.createStatement();

        try {
            final ResultSet rs = statement.executeQuery("SELECT ID, HEARTBEAT, VERSION FROM HIGHLANDER WHERE GROUPNAME = '" + groupName + "'");

            try {
                if (!rs.next()) {
                    return null;
                }

                return newElection(
                        groupName,
                        rs.getString("ID"),
                        rs.getTimestamp("HEARTBEAT"),
                        rs.getInt("VERSION"));
            } finally {
                rs.close();
            }
        } finally {
            statement.close();
        }
    }

    private Election loadOrInsertElection(String groupName, String id) throws SQLException {

        Election election = findElection(groupName);

        if (election != null) {
            return election;
        }

        try {
            return insertElection(groupName, id);
        } catch (SQLException e) {
            if (isConstraintViolation(e)) {
                return findElection(groupName);
            }
            throw new RuntimeException(e);
        }
    }

    private boolean isConstraintViolation(SQLException e) {
        return e.getSQLState().startsWith("23");
    }

    private Election insertElection(final String groupName, final String id) throws SQLException {
        PreparedStatement st = connection.prepareStatement("INSERT INTO HIGHLANDER (GROUPNAME, ID, HEARTBEAT, VERSION) VALUES (?, ?, ?, ?)");

        try {
            final Date newHeartBeat = new Date();
            final int newVersion = 0;

            st.setString(1, groupName);
            st.setString(2, id);
            st.setTimestamp(3, timestamp(newHeartBeat));
            st.setInt(4, newVersion);

            st.execute();

            return newElection(groupName, id, newHeartBeat, newVersion);
        } finally {
            st.close();
        }
    }

    private Election newElection(final String groupName, final String id, final Date heartBeat, final int newVersion) {
        return new Election() {
            {
                electionGroupName = groupName;
                electedParticipant = id;
                lastHeartBeat = heartBeat;
                version = newVersion;
            }
        };
    }

    private Timestamp timestamp(Date date) {
        return new Timestamp(date.getTime());
    }
}
