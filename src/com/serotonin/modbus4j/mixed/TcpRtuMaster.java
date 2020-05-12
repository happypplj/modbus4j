package com.serotonin.modbus4j.mixed;

import com.serotonin.modbus4j.ModbusMaster;
import com.serotonin.modbus4j.exception.ModbusInitException;
import com.serotonin.modbus4j.exception.ModbusTransportException;
import com.serotonin.modbus4j.ip.IpParameters;
import com.serotonin.modbus4j.ip.tcp.TcpMaster;
import com.serotonin.modbus4j.msg.ModbusRequest;
import com.serotonin.modbus4j.msg.ModbusResponse;
import com.serotonin.modbus4j.serial.SerialWaitingRoomKeyFactory;
import com.serotonin.modbus4j.serial.rtu.RtuMessageParser;
import com.serotonin.modbus4j.serial.rtu.RtuMessageRequest;
import com.serotonin.modbus4j.serial.rtu.RtuMessageResponse;
import com.serotonin.modbus4j.sero.io.StreamUtils;
import com.serotonin.modbus4j.sero.messaging.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * RS485通信 网口转串口 tcp连接rtu报文
 *
 * @author xxjia
 * @since
 */
public class TcpRtuMaster extends ModbusMaster {
    private static final int RETRY_PAUSE_START = 50;
    private static final int RETRY_PAUSE_MAX = 1000;

    // Configuration fields.
    private final Log LOG = LogFactory.getLog(TcpMaster.class);
    private final IpParameters ipParameters;
    private final boolean keepAlive;
    // Runtime fields.
    private Socket socket;
    private Transport transport;
    private MessageControl conn;

    public TcpRtuMaster(IpParameters params, boolean keepAlive) {
        this.ipParameters = params;
        this.keepAlive = keepAlive;
    }

    @Override
    synchronized public void init() throws ModbusInitException {
        try {
            if (keepAlive)
                openConnection();
        } catch (Exception e) {
            throw new ModbusInitException(e);
        }
        initialized = true;
    }

    @Override
    synchronized public void destroy() {
        closeConnection();
        initialized = false;
    }

    @Override
    synchronized public ModbusResponse sendImpl(ModbusRequest request) throws ModbusTransportException {
        try {
            // Check if we need to open the connection.
            if (!keepAlive)
                openConnection();
        } catch (Exception e) {
            closeConnection();
            throw new ModbusTransportException(e, request.getSlaveId());
        }

        RtuMessageRequest rtuRequest = new RtuMessageRequest(request);
        LOG.info("Modbus请求Rtu消息: " + StreamUtils.dumpHex(rtuRequest.getMessageData()));
        // Send the request to get the response.
        RtuMessageResponse rtuResponse;
        try {
            rtuResponse = (RtuMessageResponse) conn.send(rtuRequest);
            if (rtuResponse == null)
                return null;

//            if (enablePrintRawMessage) {
//                log.info("Modbus响应Rtu消息: " + StreamUtils.dumpHex(rtuResponse.getMessageData()));
//            }
            return rtuResponse.getModbusResponse();
        } catch (Exception e) {
            LOG.error("Exception: " + e.getMessage() + " " + e.getLocalizedMessage());
            if (keepAlive) {
                // The connection may have been reset, so try to reopen it and attempt the message again.
                try {
                    openConnection();
                    rtuResponse = (RtuMessageResponse) conn.send(rtuRequest);
                    if (rtuResponse == null)
                        return null;
//                    if (enablePrintRawMessage) {
//                        log.info("Modbus响应Rtu消息: " + StreamUtils.dumpHex(rtuResponse.getMessageData()));
//                    }
                    return rtuResponse.getModbusResponse();
                } catch (Exception e2) {
                    closeConnection();
                    LOG.error("Exception: " + e2.getMessage() + " " + e2.getLocalizedMessage());
                    throw new ModbusTransportException(e2, request.getSlaveId());
                }
            }

            throw new ModbusTransportException(e, request.getSlaveId());
        } finally {
            // Check if we should close the connection.
            if (!keepAlive)
                closeConnection();
        }
    }

    //
    //
    // Private methods
    //
    private void openConnection() throws IOException {
        // Make sure any existing connection is closed.
        closeConnection();

        // Try 'retries' times to get the socket open.
        int retries = getRetries();
        int retryPause = RETRY_PAUSE_START;
        while (true) {
            try {
                socket = new Socket();
                socket.setSoTimeout(getTimeout());
                socket.connect(new InetSocketAddress(ipParameters.getHost(), ipParameters.getPort()), getTimeout());
                if (getePoll() != null)
                    transport = new EpollStreamTransport(socket.getInputStream(), socket.getOutputStream(), getePoll());
                else
                    transport = new StreamTransport(socket.getInputStream(), socket.getOutputStream());
                break;
            } catch (IOException e) {
                closeConnection();

                if (retries <= 0)
                    throw e;
                retries--;

                // Pause for a bit.
                try {
                    Thread.sleep(retryPause);
                } catch (InterruptedException e1) {
                    // ignore
                }
                retryPause *= 2;
                if (retryPause > RETRY_PAUSE_MAX)
                    retryPause = RETRY_PAUSE_MAX;
            }
        }
        RtuMessageParser rtuMessageParser = new RtuMessageParser(true);
        WaitingRoomKeyFactory waitingRoomKeyFactory = new SerialWaitingRoomKeyFactory();
        conn = getMessageControl();
        conn.start(transport, rtuMessageParser, null, waitingRoomKeyFactory);
        if (getePoll() == null)
            ((StreamTransport) transport).start("Modbus4J TcpMaster");
    }

    private void closeConnection() {
        closeMessageControl(conn);
        try {
            if (socket != null)
                socket.close();
        } catch (IOException e) {
            getExceptionHandler().receivedException(e);
        }

        conn = null;
        socket = null;
    }
}


