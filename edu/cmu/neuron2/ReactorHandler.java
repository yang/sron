package edu.cmu.neuron2;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public interface ReactorHandler {

    public void handle(InetSocketAddress src, ByteBuffer buf);

}
