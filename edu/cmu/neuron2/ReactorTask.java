package edu.cmu.neuron2;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class ReactorTask implements ScheduledFuture<Void> {

    private final Runnable r;
    private final long time;

    private static enum TaskState {
        WAITING, RUNNING, DONE, CANCELLED
    };

    private TaskState state = TaskState.WAITING;

    public ReactorTask(Runnable r, long time) {
        this.r = r;
        this.time = time;
    }

    public void run() {
        r.run();
        state = TaskState.DONE;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (state == TaskState.WAITING) {
            state = TaskState.CANCELLED;
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
