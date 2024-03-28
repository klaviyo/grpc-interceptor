from grpc._typing import ChannelArgumentType
from grpc._cython import cygrpc
from typing import Optional, Sequence
from grpc import _common
from grpc.aio._channel import _augment_channel_arguments
import grpc.aio
from grpc.aio import (
    ClientInterceptor,
    UnaryUnaryClientInterceptor,
    UnaryStreamClientInterceptor,
    StreamUnaryClientInterceptor,
    StreamStreamClientInterceptor
)


class Channel(grpc.aio._channel.Channel):
    def __init__(self, target: str, options: ChannelArgumentType,
                 credentials: Optional[grpc.ChannelCredentials],
                 compression: Optional[grpc.Compression],
                 interceptors: Optional[Sequence[ClientInterceptor]]):
        """Constructor.

        Args:
          target: The target to which to connect.
          options: Configuration options for the channel.
          credentials: A cygrpc.ChannelCredentials or None.
          compression: An optional value indicating the compression method to be
            used over the lifetime of the channel.
          interceptors: An optional list of interceptors that would be used for
            intercepting any RPC executed with that channel.
        """
        self._unary_unary_interceptors = []
        self._unary_stream_interceptors = []
        self._stream_unary_interceptors = []
        self._stream_stream_interceptors = []
        valid_classes = (UnaryUnaryClientInterceptor, UnaryStreamClientInterceptor,
                         StreamUnaryClientInterceptor, StreamStreamClientInterceptor)

        if interceptors is not None:
            for interceptor in interceptors:
                if not isinstance(interceptor, valid_classes):
                    msg = ' or '.join(x.__name__ for x in valid_classes)
                    raise ValueError(
                        f"Interceptor {interceptor} must be {msg}")
                if isinstance(interceptor, UnaryUnaryClientInterceptor):
                    self._unary_unary_interceptors.append(interceptor)
                if isinstance(interceptor, UnaryStreamClientInterceptor):
                    self._unary_stream_interceptors.append(interceptor)
                if isinstance(interceptor, StreamUnaryClientInterceptor):
                    self._stream_unary_interceptors.append(interceptor)
                if isinstance(interceptor, StreamStreamClientInterceptor):
                    self._stream_stream_interceptors.append(interceptor)

        self._loop = cygrpc.get_working_loop()
        self._channel = cygrpc.AioChannel(
            _common.encode(target),
            _augment_channel_arguments(options, compression), credentials,
            self._loop)


def insecure_channel(
        target: str,
        options: Optional[ChannelArgumentType] = None,
        compression: Optional[grpc.Compression] = None,
        interceptors: Optional[Sequence[ClientInterceptor]] = None):
    """Creates an insecure asynchronous Channel to a server.

    Args:
      target: The server address
      options: An optional list of key-value pairs (:term:`channel_arguments`
        in gRPC Core runtime) to configure the channel.
      compression: An optional value indicating the compression method to be
        used over the lifetime of the channel.
      interceptors: An optional sequence of interceptors that will be executed for
        any call executed with this channel.

    Returns:
      A Channel.
    """
    return Channel(target, () if options is None else options, None,
                   compression, interceptors)


def secure_channel(target: str,
                   credentials: grpc.ChannelCredentials,
                   options: Optional[ChannelArgumentType] = None,
                   compression: Optional[grpc.Compression] = None,
                   interceptors: Optional[Sequence[ClientInterceptor]] = None):
    """Creates a secure asynchronous Channel to a server.

    Args:
      target: The server address.
      credentials: A ChannelCredentials instance.
      options: An optional list of key-value pairs (:term:`channel_arguments`
        in gRPC Core runtime) to configure the channel.
      compression: An optional value indicating the compression method to be
        used over the lifetime of the channel.
      interceptors: An optional sequence of interceptors that will be executed for
        any call executed with this channel.

    Returns:
      An aio.Channel.
    """
    return Channel(target, () if options is None else options,
                   credentials._credentials, compression, interceptors)
