package github.abendt.highlander.jdbc;

import org.junit.Rule;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.api.Assertions.contentOf;

public class JdbcElectionTest {

    @Rule
    public final HighlanderInMemoryDbRule database = new HighlanderInMemoryDbRule();

    @Test
    public void oneParticipantWinsOnEmptyDatabase() {
        JdbcElection jdbcElection = new JdbcElection("Group");

        assertThat(jdbcElection.runElection(database.getConnection())).isTrue();
    }

    @Test
    public void whenParticipantIsNotLeaderHeartBeatIsUnchanged() throws Exception {
        JdbcElection election1 = new JdbcElection("Group");
        JdbcElection election2 = new JdbcElection("Group");

        election1.runElection(database.getConnection());

        Date firstHeartBeat = database.getHeartBean("Group");

        election2.runElection(database.getConnection());

        Date nextHeartBeat = database.getHeartBean("Group");

        assertThat(nextHeartBeat).isEqualTo(firstHeartBeat);
    }

    @Test
    public void whenParticipantIsLeaderHeartBeatIsUpdatedOnEveryElection() throws Exception {
        JdbcElection election = new JdbcElection("Group");

        election.runElection(database.getConnection());

        Date firstHeartBeat = database.getHeartBean("Group");

        election.runElection(database.getConnection());

        Date nextHeartBeat = database.getHeartBean("Group");

        assertThat(nextHeartBeat).isAfter(firstHeartBeat);
    }

    @Test
    public void onePartipantMayElectMultipleTimes() {
        JdbcElection election = new JdbcElection("Group");

        assertThat(election.runElection(database.getConnection())).isTrue();
        assertThat(election.runElection(database.getConnection())).isTrue();
    }

    @Test
    public void withTwoParticipantsSecondOneLooses() {
        JdbcElection election1 = new JdbcElection("Group");
        JdbcElection election2 = new JdbcElection("Group");

        assertThat(election1.runElection(database.getConnection())).isTrue();
        assertThat(election2.runElection(database.getConnection())).isFalse();
    }

    @Test
    public void participantCanGiveUpLeadership() {
        JdbcElection election1 = new JdbcElection("Group");
        JdbcElection election2 = new JdbcElection("Group");

        election1.runElection(database.getConnection());
        election1.giveUpLeaderShip(database.getConnection());

        assertThat(election2.runElection(database.getConnection())).isTrue();
    }

    @Test
    public void participantsInDifferentGroupsCanCoExist() {
        JdbcElection election1 = new JdbcElection("Group1");
        JdbcElection election2 = new JdbcElection("Group2");

        assertThat(election1.runElection(database.getConnection())).isTrue();
        assertThat(election2.runElection(database.getConnection())).isTrue();
    }

    @Test
    public void winsElectionWhenPreviousLeaderIsNull() throws SQLException {
        database.insertElection("Group", null, null);
        JdbcElection election1 = new JdbcElection("Group");

        assertThat(election1.runElection(database.getConnection())).isTrue();
    }

    @Test
    public void winsElectionWhenPreviousHeartBeatIsTooOld() throws SQLException {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.HOUR, -1);

        database.insertElection("Group", "SomeOne", cal.getTime());
        JdbcElection election1 = new JdbcElection("Group");

        assertThat(election1.runElection(database.getConnection())).isTrue();
    }

    @Test
    public void canDetermineConstraintViolation() throws Exception {
        database.insertElection("Group", null, null);

        try {
            database.insertElection("Group", null, null);

        } catch (SQLException e) {
            assertThat(e.getSQLState()).startsWith("23");
        }
    }

    List<String> result = new ArrayList();

//    @Test
    public void loadTest() throws Exception {
        List<Thread> list = new ArrayList();
        for (int x=0;x<50; ++x) {
            list.add(new Thread(newRunnable(x)));
        }

        for (Thread t: list) {
            t.start();
        }

        for (Thread t: list) {
            t.join();
        }

        System.out.println(result.size());
    }

    private Runnable newRunnable(final int count) {
        return new Runnable() {
            @Override
            public void run() {
                JdbcElection jdbcElection = new JdbcElection("MyGroup");

                for (int x=0; x<10000; ++x) {
                    jdbcElection.runElection(database.getConnection());
                }

                result.add("OK " + count);
            }
        };
    }

}
