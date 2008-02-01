package edu.cmu.neuron2;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class Reactor {

    private final Selector selector;
    private final List<ReactorService> services = new ArrayList<ReactorService>();

    public Reactor() throws Exception {
        selector = Selector.open();
    }

    public ReactorService register(InetSocketAddress remoteSa, InetSocketAddress localSa, ReactorHandler handler) throws Exception {
        ReactorService service = new ReactorService(remoteSa, localSa, handler, services.size(), selector);
        services.add(service);
        return service;
    }

    public void react()
            throws Exception {
        while (true) {
            selector.select();

            Set<SelectionKey> keys = selector.selectedKeys();
            for (SelectionKey key : keys) {
                if (key.isValid()) {
                    if (key.isReadable()) {
                        ((ReactorService) key.attachment()).read(key);
                    } else if (key.isWritable()) {
                        ((ReactorService) key.attachment()).write(key);
                    }
                }
            }
            keys.clear();
        }
    }

}
