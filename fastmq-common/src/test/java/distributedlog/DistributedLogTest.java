//package distributedlog;
//
//import com.twitter.finagle.thrift.ClientId;
//import com.twitter.util.Future;
//import java.net.URI;
//import java.nio.ByteBuffer;
//import java.util.Arrays;
//import java.util.concurrent.TimeUnit;
//import org.apache.distributedlog.DLSN;
//import org.apache.distributedlog.DistributedLogConfiguration;
//import org.apache.distributedlog.DistributedLogConstants;
//import org.apache.distributedlog.LogRecord;
//import org.apache.distributedlog.LogRecordWithDLSN;
//import org.apache.distributedlog.api.AsyncLogWriter;
//import org.apache.distributedlog.api.DistributedLogManager;
//import org.apache.distributedlog.api.namespace.Namespace;
//import org.apache.distributedlog.api.namespace.NamespaceBuilder;
//import org.apache.distributedlog.client.DistributedLogMultiStreamWriter;
//import org.apache.distributedlog.common.concurrent.FutureUtils;
//import org.apache.distributedlog.service.DistributedLogClient;
//import org.apache.distributedlog.service.DistributedLogClientBuilder;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * Created by song on 下午5:44.
// */
//public class DistributedLogTest {
//
//    private static final Logger logger = LoggerFactory.getLogger(DistributedLogTest.class);
//
//    /**
//     * 1. 写分片
//     * 2. 设计细节、架构
//     *
//     * @throws Exception
//     */
//    @Test
//    public void write() throws Exception {
//
//        DistributedLogManager dlm = null;
//        Namespace namespace = null;
//        AsyncLogWriter asyncLogWriter = null;
//        try {
//            URI uri = URI.create("distributedlog://127.0.0.1:7000/messaging/my_namespace");
//            DistributedLogConfiguration conf = new DistributedLogConfiguration();
//            conf.setImmediateFlushEnabled(true);
//            conf.setOutputBufferSize(0);
//            conf.setPeriodicFlushFrequencyMilliSeconds(0);
//            conf.setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE);
//
//            namespace = NamespaceBuilder.newBuilder()
//                .conf(conf)
//                .uri(uri)
//                .regionId(DistributedLogConstants.LOCAL_REGION_ID)
//                .clientId("console-writer")
//                .build();
//
//            dlm = namespace.openLog("basic-stream-1");
//            asyncLogWriter = FutureUtils.result(dlm.openAsyncLogWriter());
//            LogRecord logRecord = new LogRecord(System.currentTimeMillis(), "Hello World".getBytes());
//            DLSN dlsn = asyncLogWriter.write(logRecord).get();
//            System.out.println(dlsn);
//        } finally {
//            if (null != asyncLogWriter) {
//                FutureUtils.result(asyncLogWriter.asyncClose(), 5, TimeUnit.SECONDS);
//            }
//            if (dlm != null) {
//                dlm.close();
//            }
//            if (namespace != null) {
//                namespace.close();
//            }
//        }
//    }
//
//    @Test
//    public void read() throws Exception {
//        DistributedLogManager dlm = null;
//        Namespace namespace = null;
//        AsyncLogWriter asyncLogWriter = null;
//        try {
//            URI uri = URI.create("distributedlog://127.0.0.1:7000/messaging/my_namespace");
//            DistributedLogConfiguration conf = new DistributedLogConfiguration();
//            conf.setImmediateFlushEnabled(true);
//            conf.setOutputBufferSize(0);
//            conf.setPeriodicFlushFrequencyMilliSeconds(0);
//            conf.setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE);
//
//            namespace = NamespaceBuilder.newBuilder()
//                .conf(conf)
//                .uri(uri)
//                .regionId(DistributedLogConstants.LOCAL_REGION_ID)
//                .clientId("console-writer")
//                .build();
//            dlm = namespace.openLog("basic-stream-1");
//            LogRecordWithDLSN record = dlm.getLastLogRecord();
//            byte[] payload = record.getPayload();
//            System.out.println(payload);
//            System.out.println(new String(payload));
//            DLSN dlsn = record.getDlsn();
//            System.out.println(dlsn);
//        } finally {
//            if (dlm != null) {
//                dlm.close();
//            }
//            if (namespace != null) {
//                namespace.close();
//            }
//        }
//    }
//
//    @Test
//    public void proxyWrite() throws Exception {
//        DistributedLogClientBuilder builder = DistributedLogClientBuilder.newBuilder()
//            .clientId(ClientId.apply("console-proxy-writer"))
//            .name("console-proxy-writer");
//        builder = builder.thriftmux(true);
//        String finagleNameStr = "inet!127.0.0.1:8000";
//        builder = builder.finagleNameStr(finagleNameStr);
//        DistributedLogClient client = builder.build();
//        DistributedLogMultiStreamWriter multiStreamWriter = DistributedLogMultiStreamWriter.newBuilder()
//            .streams(Arrays.asList("basic-stream-1", "basic-stream-2"))
//            .client(client)
//            .bufferSize(0)
//            .flushIntervalMs(0)
//            .firstSpeculativeTimeoutMs(10000)
//            .maxSpeculativeTimeoutMs(20000)
//            .requestTimeoutMs(50000)
//            .build();
//
//        Future<DLSN> write = multiStreamWriter.write(ByteBuffer.wrap("Hello World".getBytes()));
//        //DLSN{logSegmentSequenceNo=1, entryId=4, slotId=0}
//        DLSN dlsn = write.get();
//        System.out.println(dlsn);
//        client.close();
//    }
//}
