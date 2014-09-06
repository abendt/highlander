package github.abendt.highlander.jdbc;

import github.abendt.highlander.ElectionListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.jayway.awaitility.Awaitility.await;
import static org.fest.assertions.api.Assertions.assertThat;

public class HighlanderTest {

    @Rule
    public final HighlanderInMemoryDbRule database = new HighlanderInMemoryDbRule();

    private static int count;
    private String uniqueGroupName;

    @Before
    public void provideUniqueGroupName() {
        uniqueGroupName = "Group-" + (count++);
    }

    @After
    public void stopHighlanders() {
        for (Highlander h: highlanderLeaderMap.keySet()) {
            h.stop();
        }
    }

    private final Map<Highlander, Boolean> highlanderLeaderMap = new HashMap();

    private Highlander newHighlander() {
        Highlander highlander = new Highlander(database.getDataSource(), uniqueGroupName);

        addListener(highlander);

        return highlander;
    }

    private void addListener(final Highlander highlander) {
        highlander.setListener(new ElectionListener() {
            @Override
            public void groupChanged(boolean leader) {
                highlanderLeaderMap.put(highlander, leader);
            }
        });
    }

    @Test
    public void canRunSingleElection() {
        Highlander highlander = newHighlander();

        highlander.runElection();

        assertThat(highlanderLeaderMap.get(highlander)).isTrue();
    }

    @Test
    public void secondHighlanderTakesOverIfFirstOneIsNotAlive() {
        final Highlander highlander1 = newHighlander();
        final Highlander highlander2 = newHighlander();

        highlander1.runElection();

        highlander2.start();

        await().until(highlanderIsElected(highlander2));
    }

    @Test
    public void secondHighlanderTakesOverIfFirstOneIsStopped() {
        final Highlander highlander1 = newHighlander();
        final Highlander highlander2 = newHighlander();

        highlander1.start();

        await().until(highlanderIsElected(highlander1));

        highlander2.start();
        highlander1.stop();

        await().until(highlanderIsElected(highlander2));
    }

    private Callable<Boolean> highlanderIsElected(final Highlander highlander) {
        return new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return highlanderLeaderMap.get(highlander) == Boolean.TRUE;
            }
        };
    }
}