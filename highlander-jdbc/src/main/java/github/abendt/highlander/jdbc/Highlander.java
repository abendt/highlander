package github.abendt.highlander.jdbc;

import github.abendt.highlander.ElectionListener;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Timer;
import java.util.TimerTask;


public class Highlander {

    Timer timer;

    int heartBeatInterval = 1000;
    int heartBeatFactor = 3;

    private DataSource dataSource;
    private JdbcElection jdbcElection;

    private boolean leader;
    private ElectionListener electionListener;
    private TimerTask task;

    public Highlander(DataSource dataSource, String groupName) {
        this.dataSource = dataSource;
        jdbcElection = new JdbcElection(groupName);
        jdbcElection.setMaxHeartBeatAge(heartBeatFactor * heartBeatInterval);
    }

    public void setListener(ElectionListener listener) {
        this.electionListener = listener;
    }

    public void start() {
        timer = new Timer();
        task = new TimerTask() {
            @Override
            public void run() {
                runElection();
            }
        };
        timer.scheduleAtFixedRate(task, heartBeatInterval, heartBeatInterval);
    }

    public void stop() {
        if (leader) {
            electionListener.groupChanged(false);
        }

        if (task != null) {
            task.cancel();
        }

        if (timer != null) {
            timer.cancel();
        }

        try {
            Connection connection = dataSource.getConnection();

            try {
                jdbcElection.giveUpLeaderShip(connection);

            } finally {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void runElection() {
        try {
            Connection connection = dataSource.getConnection();

            try {
                boolean result = jdbcElection.runElection(connection);

                announceResult(result);
            } finally {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void announceResult(boolean result) {
        leader = result;

        if (electionListener != null) {
            electionListener.groupChanged(result);
        }
    }

}
