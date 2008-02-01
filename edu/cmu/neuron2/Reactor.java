package edu.cmu.neuron2;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

class Reactor {

    private final Selector selector;
    private final List<Session> services = new ArrayList<Session>();
    private AtomicBoolean doShutdown = new AtomicBoolean(false);

    public Reactor() throws Exception {
        selector = Selector.open();
    }

    public Session register(InetSocketAddress remoteSa, InetSocketAddress localSa, ReactorHandler handler) {
        Session service = new Session(remoteSa, localSa, handler, services.size(), selector);
        services.add(service);
        return service;
    }

    public void react()
            throws Exception {
        while (true) {
            selector.select();
            if (doShutdown.get())
                break;

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
        }
        selector.close();
    }

    /**
     * Safe to call from any thread.
     */
    public void shutdown() {
        doShutdown.set(true);
        selector.wakeup();
    }

}
