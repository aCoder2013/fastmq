package com.song.fastmq.net.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.9.0)",
    comments = "Source: brokerRemotingService.proto")
public final class BrokerRemotingServiceGrpc {

  private BrokerRemotingServiceGrpc() {}

  private static <T> io.grpc.stub.StreamObserver<T> toObserver(final io.vertx.core.Handler<io.vertx.core.AsyncResult<T>> handler) {
    return new io.grpc.stub.StreamObserver<T>() {
      private volatile boolean resolved = false;
      @Override
      public void onNext(T value) {
        if (!resolved) {
          resolved = true;
          handler.handle(io.vertx.core.Future.succeededFuture(value));
        }
      }

      @Override
      public void onError(Throwable t) {
        if (!resolved) {
          resolved = true;
          handler.handle(io.vertx.core.Future.failedFuture(t));
        }
      }

      @Override
      public void onCompleted() {
        if (!resolved) {
          resolved = true;
          handler.handle(io.vertx.core.Future.succeededFuture());
        }
      }
    };
  }

  public static final String SERVICE_NAME = "com.song.fastmq.broker.net.BrokerRemotingService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getHandleProducerRequestMethod()} instead. 
  public static final io.grpc.MethodDescriptor<com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest,
      com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply> METHOD_HANDLE_PRODUCER_REQUEST = getHandleProducerRequestMethod();

  private static volatile io.grpc.MethodDescriptor<com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest,
      com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply> getHandleProducerRequestMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest,
      com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply> getHandleProducerRequestMethod() {
    io.grpc.MethodDescriptor<com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest, com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply> getHandleProducerRequestMethod;
    if ((getHandleProducerRequestMethod = BrokerRemotingServiceGrpc.getHandleProducerRequestMethod) == null) {
      synchronized (BrokerRemotingServiceGrpc.class) {
        if ((getHandleProducerRequestMethod = BrokerRemotingServiceGrpc.getHandleProducerRequestMethod) == null) {
          BrokerRemotingServiceGrpc.getHandleProducerRequestMethod = getHandleProducerRequestMethod = 
              io.grpc.MethodDescriptor.<com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest, com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "com.song.fastmq.broker.net.BrokerRemotingService", "handleProducerRequest"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply.getDefaultInstance()))
                  .setSchemaDescriptor(new BrokerRemotingServiceMethodDescriptorSupplier("handleProducerRequest"))
                  .build();
          }
        }
     }
     return getHandleProducerRequestMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BrokerRemotingServiceStub newStub(io.grpc.Channel channel) {
    return new BrokerRemotingServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BrokerRemotingServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BrokerRemotingServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BrokerRemotingServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BrokerRemotingServiceFutureStub(channel);
  }

  /**
   * Creates a new vertx stub that supports all call types for the service
   */
  public static BrokerRemotingServiceVertxStub newVertxStub(io.grpc.Channel channel) {
    return new BrokerRemotingServiceVertxStub(channel);
  }

  /**
   */
  public static abstract class BrokerRemotingServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void handleProducerRequest(com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest request,
        io.grpc.stub.StreamObserver<com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply> responseObserver) {
      asyncUnimplementedUnaryCall(getHandleProducerRequestMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getHandleProducerRequestMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest,
                com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply>(
                  this, METHODID_HANDLE_PRODUCER_REQUEST)))
          .build();
    }
  }

  /**
   */
  public static final class BrokerRemotingServiceStub extends io.grpc.stub.AbstractStub<BrokerRemotingServiceStub> {
    public BrokerRemotingServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    public BrokerRemotingServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BrokerRemotingServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BrokerRemotingServiceStub(channel, callOptions);
    }

    /**
     */
    public void handleProducerRequest(com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest request,
        io.grpc.stub.StreamObserver<com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getHandleProducerRequestMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class BrokerRemotingServiceBlockingStub extends io.grpc.stub.AbstractStub<BrokerRemotingServiceBlockingStub> {
    public BrokerRemotingServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    public BrokerRemotingServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BrokerRemotingServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BrokerRemotingServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply handleProducerRequest(com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest request) {
      return blockingUnaryCall(
          getChannel(), getHandleProducerRequestMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class BrokerRemotingServiceFutureStub extends io.grpc.stub.AbstractStub<BrokerRemotingServiceFutureStub> {
    public BrokerRemotingServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    public BrokerRemotingServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BrokerRemotingServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BrokerRemotingServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply> handleProducerRequest(
        com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getHandleProducerRequestMethod(), getCallOptions()), request);
    }
  }

  /**
   */
  public static abstract class BrokerRemotingServiceVertxImplBase implements io.grpc.BindableService {

    /**
     */
    public void handleProducerRequest(com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest request,
        io.vertx.core.Future<com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply> response) {
      asyncUnimplementedUnaryCall(getHandleProducerRequestMethod(), BrokerRemotingServiceGrpc.toObserver(response.completer()));
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getHandleProducerRequestMethod(),
            asyncUnaryCall(
              new VertxMethodHandlers<
                com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest,
                com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply>(
                  this, METHODID_HANDLE_PRODUCER_REQUEST)))
          .build();
    }
  }

  /**
   */
  public static final class BrokerRemotingServiceVertxStub extends io.grpc.stub.AbstractStub<BrokerRemotingServiceVertxStub> {
    public BrokerRemotingServiceVertxStub(io.grpc.Channel channel) {
      super(channel);
    }

    public BrokerRemotingServiceVertxStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BrokerRemotingServiceVertxStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BrokerRemotingServiceVertxStub(channel, callOptions);
    }

    /**
     */
    public void handleProducerRequest(com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest request,
        io.vertx.core.Handler<io.vertx.core.AsyncResult<com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply>> response) {
      asyncUnaryCall(
          getChannel().newCall(getHandleProducerRequestMethod(), getCallOptions()), request, BrokerRemotingServiceGrpc.toObserver(response));
    }
  }

  private static final int METHODID_HANDLE_PRODUCER_REQUEST = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BrokerRemotingServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BrokerRemotingServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_HANDLE_PRODUCER_REQUEST:
          serviceImpl.handleProducerRequest((com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest) request,
              (io.grpc.stub.StreamObserver<com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static final class VertxMethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BrokerRemotingServiceVertxImplBase serviceImpl;
    private final int methodId;

    VertxMethodHandlers(BrokerRemotingServiceVertxImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_HANDLE_PRODUCER_REQUEST:
          serviceImpl.handleProducerRequest((com.song.fastmq.net.proto.BrokerRemotingApi.ProducerRequest) request,
              (io.vertx.core.Future<com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply>) io.vertx.core.Future.<com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply>future().setHandler(ar -> {
                if (ar.succeeded()) {
                  ((io.grpc.stub.StreamObserver<com.song.fastmq.net.proto.BrokerRemotingApi.ProducerSuccessReply>) responseObserver).onNext(ar.result());
                  responseObserver.onCompleted();
                } else {
                  responseObserver.onError(ar.cause());
                }
              }));
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class BrokerRemotingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BrokerRemotingServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.song.fastmq.net.proto.BrokerRemotingApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BrokerRemotingService");
    }
  }

  private static final class BrokerRemotingServiceFileDescriptorSupplier
      extends BrokerRemotingServiceBaseDescriptorSupplier {
    BrokerRemotingServiceFileDescriptorSupplier() {}
  }

  private static final class BrokerRemotingServiceMethodDescriptorSupplier
      extends BrokerRemotingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BrokerRemotingServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (BrokerRemotingServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BrokerRemotingServiceFileDescriptorSupplier())
              .addMethod(getHandleProducerRequestMethod())
              .build();
        }
      }
    }
    return result;
  }
}
