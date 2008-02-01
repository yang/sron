package edu.cmu.neuron2;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

class Reactor {

    private final List<ByteBuffer> pendingWrites;
    private final Selector selector;
    private final DatagramChannel channel;
    private final InetSocketAddress remoteSa, localSa;
    private final ReactorHandler handler;

    public Reactor(InetSocketAddress remoteSa, InetSocketAddress localSa, ReactorHandler handler) throws Exception {
        pendingWrites = new ArrayList<ByteBuffer>();
        selector = Selector.open();
        channel = DatagramChannel.open();
        this.remoteSa = remoteSa;
        this.localSa = localSa;
        this.handler = handler;

        channel.configureBlocking(false);
        DatagramSocket socket = channel.socket();
        socket.setReuseAddress(true);
        log(localSa);
        if (localSa != null)
            socket.bind(localSa);
        if (remoteSa != null)
            channel.connect(remoteSa);
        channel.register(selector, SelectionKey.OP_READ);

        log("Listening on socket " + socket.getLocalAddress() + ":"
                + socket.getLocalPort());

    }

    private void log(Object msg) {
        System.out.println(msg);
    }

    public void send(ByteBuffer writeBuf, InetSocketAddress dst) throws Exception {
        // selector.wakeup();
        channel.send(writeBuf, dst);
    }

    public void react()
            throws Exception {
        final ByteBuffer readBuf = ByteBuffer.allocateDirect(4096);

        while (true) {
            selector.select();

            Set<SelectionKey> keys = selector.selectedKeys();
            for (SelectionKey key : keys) {
                if (key.isValid()) {
                    if (key.isReadable()) {
                        try {
                            InetSocketAddress srcSa = (InetSocketAddress) channel
                                    .receive(readBuf);
                            if (false) {
                                int numRead = 0;
                                if (numRead == -1) {
                                    // Remote entity shut the socket down
                                    // cleanly.
                                    // Do
                                    // the same from our end and cancel the
                                    // channel.
                                    key.channel().close();
                                    key.cancel();
                                }
                            }
                            // worker
                            handler.handle(srcSa, readBuf);
                            // recycle buffer
                            readBuf.clear();
                        } catch (IOException e) {
                            // The remote forcibly closed the connection, cancel
                            // the selection key and close the channel.
                            key.cancel();
                            channel.close();
                        }
                    } else if (key.isWritable()) {
                        List<ByteBuffer> queue = pendingWrites;

                        // Write until there's not more data ...
                        while (!queue.isEmpty()) {
                            ByteBuffer buf = (ByteBuffer) queue.get(0);
                            channel.write(buf);
                            if (buf.remaining() > 0) {
                                // ... or the socket's buffer fills up
                                break;
                            }
                            queue.remove(0);
                        }

                        if (queue.isEmpty()) {
                            // We wrote away all data, so we're no longer
                            // interested
                            // in writing on this socket. Switch back to waiting
                            // for
                            // data.
                            key.interestOps(SelectionKey.OP_READ);
                        }
                    }
                }
            }
            keys.clear();
        }
    }

    // TRASH

    // set input parameters
    String host = "localhost"; // "slinky.cs.nyu.edu";
    int port = 10000;
    int datagramSize = 512;
    int datagramInterval = 3000;
    int numMsgs = 24;

    // array of bytes for a datagram to send
    byte[] sendData = new byte[datagramSize];
    ByteBuffer theSendDataByteBuffer = ByteBuffer.wrap(sendData);

    // array of bytes for receiving datagrams
    byte[] receiveData = new byte[datagramSize];
    ByteBuffer theReceiveDataByteBuffer = ByteBuffer.wrap(receiveData);

    public void doit() {
        try {
            InetAddress localhost = InetAddress.getLocalHost();

            Random theRandom = new Random();
            InetSocketAddress theInetSocketAddress = new InetSocketAddress(
                    host, port);

            // make a DatagramChannel
            DatagramChannel theDatagramChannel = DatagramChannel.open();

            // A channel must first be placed in nonblocking mode
            // before it can be registered with a selector
            theDatagramChannel.configureBlocking(false);

            // instantiate a selector
            Selector theSelector = Selector.open();

            DatagramSocket s = theDatagramChannel.socket();
            s.setReuseAddress(true);
            s.bind(new InetSocketAddress(localhost, 10000));

            // register the selector on the channel to monitor reading
            // datagrams on the DatagramChannel
            theDatagramChannel.register(theSelector, SelectionKey.OP_READ);

            long millisecsUntilSendNextDatagram = 0;
            int i = 1;
            int j = 1;

            // send and read concurrently, but do not block on read:
            while (true) {
                long start = System.currentTimeMillis();

                // which comes first, next send or a read?
                // in case millisecsUntilSendNextDatagram <= 0 go right to send
                if (millisecsUntilSendNextDatagram <= 0
                        || theSelector.select(millisecsUntilSendNextDatagram) == 0) {
                    // just for fun, send between 0 and 4 datagrams
                    for (int k = 0; k < theRandom.nextInt(5); k++) {
                        theDatagramChannel.send(theSendDataByteBuffer,
                                theInetSocketAddress);
                        System.out.println("sent Datagram " + j++);
                    }
                    millisecsUntilSendNextDatagram = datagramInterval;
                } else {

                    // read the datagram from the DatagramChannel,
                    theDatagramChannel.receive(theReceiveDataByteBuffer);
                    System.out.println("theDatagramChannel.receive " + i++);
                    // datagram would be processed here

                    // Get an iterator over the set of selected keys
                    Iterator<SelectionKey> it = theSelector.selectedKeys()
                            .iterator();

                    // will be exactly one key in the set, but iterator is
                    // only way to get at it
                    while (it.hasNext()) {
                        it.next();
                        // Remove key from selected set; it's been handled
                        it.remove();
                    }

                    // how much time used up
                    millisecsUntilSendNextDatagram -= System
                            .currentTimeMillis()
                            - start;
                }
                if (j > numMsgs)
                    break;
            }
            theSelector.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception " + e);
            return;
        }
    }

}
