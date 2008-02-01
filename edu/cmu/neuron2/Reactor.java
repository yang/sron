package edu.cmu.neuron2;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class Reactor {

    private final Selector selector;
    private final List<Session> services = new ArrayList<Session>();

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
            System.out.println("selecting");
            selector.select();
            System.out.println("selected");

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
    }

}
