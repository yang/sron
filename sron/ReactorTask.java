package sron;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class ReactorTask implements ScheduledFuture<Void> {

    private final Runnable r;
    private final long time;
    private final Reactor reactor;

    private static enum TaskState {
        WAITING, RUNNING, DONE, CANCELLED
    };

    private TaskState state = TaskState.WAITING;

    public ReactorTask(Runnable r, long time, Reactor reactor) {
        this.r = r;
        this.time = time;
        this.reactor = reactor;
    }

    public void run() {
        if (state != TaskState.CANCELLED) {
            assert state == TaskState.WAITING;
            state = TaskState.RUNNING;
            try {
                r.run();
            } catch (Exception ex) {
            }
            state = TaskState.DONE;
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (state == TaskState.WAITING) {
            state = TaskState.CANCELLED;
            boolean b = reactor.cancel(this);
            assert b;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        // TODO is this correct?
        return null;
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        throw new NotImplementedException();
    }

    @Override
    public boolean isCancelled() {
        return state == TaskState.CANCELLED;
    }

    @Override
    public boolean isDone() {
        return state == TaskState.DONE;
    }

    @Override
    public long getDelay(TimeUnit units) {
        long delay = time - System.currentTimeMillis();
        return delay > 0 ? TimeUnit.MILLISECONDS.convert(delay, units) : 0;
    }

    @Override
    public int compareTo(Delayed o) {
        if (false && o instanceof ReactorTask) {
            ReactorTask other = (ReactorTask) o;
            System.out.println(time + " vs " + other.time + " "
                    + getDelay(TimeUnit.MILLISECONDS) + " vs "
                    + other.getDelay(TimeUnit.MILLISECONDS));
            return Long.valueOf(time).compareTo(other.time);
        } else {
            return Long.valueOf(getDelay(TimeUnit.MILLISECONDS)).compareTo(
                    o.getDelay(TimeUnit.MILLISECONDS));
        }
    }

}
