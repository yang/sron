package edu.cmu.neuron2;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class Reactor {

    private final Selector selector;
    private final List<Session> services = new ArrayList<Session>();
    private boolean doShutdown = false;
    private final PriorityQueue<ReactorTask> tasks = new PriorityQueue<ReactorTask>();

    public Reactor() throws Exception {
        selector = Selector.open();
    }

    public Session register(InetSocketAddress remoteSa,
            InetSocketAddress localSa, ReactorHandler handler) {
        Session service = new Session(remoteSa, localSa, handler, services
                .size(), selector);
        services.add(service);
        return service;
    }

    public void react() throws Exception {
        while (true) {
            if (doShutdown)
                break;

            int updated;
            if (tasks.isEmpty()) {
                updated = selector.select();
            } else {
                long t = tasks.peek().getDelay(TimeUnit.MILLISECONDS);
                updated = t > 0 ? selector.select(t) : 0;
            }

            if (updated > 0) {
                Set<SelectionKey> keys = selector.selectedKeys();
                for (SelectionKey key : keys) {
                    if (key.isValid()) {
                        if (key.isReadable()) {
                            ((Session) key.attachment()).read(key);
                        } else if (key.isWritable()) {
                            ((Session) key.attachment()).write(key);
                        }
                    }
                }
                keys.clear();
            } else {
                // TODO impose limit on # things to run at once (perhaps even
                // specify costs)
                while (!tasks.isEmpty()
                        && tasks.peek().getDelay(TimeUnit.MILLISECONDS) == 0L) {
                    ReactorTask task = tasks.remove();
                    task.run();
                }
            }
        }
        selector.close();
    }

    public ScheduledFuture<?> schedule(Runnable r, long delay, TimeUnit units) {
        ReactorTask task = new ReactorTask(r, System.currentTimeMillis()
                + units.toMillis(delay));
        tasks.add(task);
        return task;
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable r,
            final long initialDelay, final long delay, final TimeUnit units) {
        return schedule(new Runnable() {
            public void run() {
                r.run();
                Reactor.this.schedule(this, delay, units);
            }
        }, initialDelay, units);
    }

    public void shutdown() {
        doShutdown = true;
    }

}
