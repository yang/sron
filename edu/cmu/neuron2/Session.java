package edu.cmu.neuron2;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;

public class Session {

    public final DatagramChannel channel;
    public final ReactorHandler handler;
    public final InetSocketAddress remoteSa, localSa;
    public final int index;
    // public final ByteBuffer readBuf = ByteBuffer.allocate(4096);
    public final ByteBuffer readBuf = ByteBuffer.allocateDirect(4096);
    public final List<ByteBuffer> pendingWrites = new ArrayList<ByteBuffer>();

    public Session(InetSocketAddress remoteSa, InetSocketAddress localSa,
            ReactorHandler handler, int index, Selector selector) {
        this.handler = handler;
        this.remoteSa = remoteSa;
        this.localSa = localSa;
        this.index = index;

        try {
            channel = DatagramChannel.open();
            channel.configureBlocking(false);
            DatagramSocket socket = channel.socket();
            socket.setReuseAddress(true);
            if (localSa != null)
                socket.bind(localSa);
            if (remoteSa != null)
                channel.connect(remoteSa);

            channel.register(selector, SelectionKey.OP_READ, this);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    // read messages is the priority
    public void read(SelectionKey key) throws Exception {
        while (true) {
            try {
                InetSocketAddress srcSa;

                if (remoteSa == null) {
                    srcSa = (InetSocketAddress) channel.receive(readBuf);
                } else {
                    int numRead = channel.read(readBuf);
                    if (numRead == -1) {
                        // Remote entity shut the socket down
                        // cleanly.
                        // Do
                        // the same from our end and cancel the
                        // channel.
                        key.channel().close();
                        key.cancel();
                    }
                    // TODO also handle numRead == 0
                    srcSa = remoteSa;
                }

                if (srcSa == null) {
                    break;
                }

                // after channel wrote to buf, set lim = pos, then pos = 0
                readBuf.flip();
                // callback
                handler.handle(this, srcSa, readBuf);
                // recycle buffer
                readBuf.clear();
            } catch (IOException e) {
                // The remote forcibly closed the connection, cancel
                // the selection key and close the channel.
                key.cancel();
                channel.close();
            }
        }
    }

    public void write(SelectionKey key) throws Exception {
        // Write until there's not more data ...
        while (!pendingWrites.isEmpty()) {
            ByteBuffer buf = (ByteBuffer) pendingWrites.get(0);
            channel.write(buf);
            if (buf.remaining() > 0) {
                // ... or the socket's buffer fills up
                break;
            }
            pendingWrites.remove(0);
        }

        if (pendingWrites.isEmpty()) {
            // We wrote away all data, so we're no longer
            // interested
            // in writing on this socket. Switch back to waiting
            // for
            // data.
            key.interestOps(SelectionKey.OP_READ);
        }
    }

    public void send(ByteBuffer writeBuf, InetSocketAddress dst)
            throws Exception {
        int bytes = channel.send(writeBuf, dst);
        assert bytes == writeBuf.limit();
    }

    public void close() throws Exception {
        channel.close();
    }

}
