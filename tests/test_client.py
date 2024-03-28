"""Test cases for the grpc-interceptor base ClientInterceptor."""

from collections import defaultdict
import itertools
from typing import List, Tuple

import grpc
import pytest

from grpc_interceptor import ClientInterceptor, AsyncClientInterceptor
from grpc_interceptor.testing import dummy_client, DummyRequest, raises


class MetadataInterceptor(ClientInterceptor):
    """A test interceptor that injects invocation metadata."""

    def __init__(self, metadata: List[Tuple[str, str]]):
        self._metadata = metadata

    def intercept(self, method, request_or_iterator, call_details):
        """Add invocation metadata to request."""
        new_details = call_details._replace(metadata=self._metadata)
        return method(request_or_iterator, new_details)


class AsyncMetadataInterceptor(AsyncClientInterceptor):
    """A test interceptor that injects invocation metadata."""

    def __init__(self, metadata: List[Tuple[str, str]]):
        self._metadata = metadata

    async def intercept(self, method, request_or_iterator, client_call_details):
        """Add invocation metadata to request."""
        new_details = client_call_details._replace(metadata=self._metadata)
        return await method(request_or_iterator, new_details)


class CodeCountInterceptor(ClientInterceptor):
    """Test interceptor that counts status codes returned by the server."""

    def __init__(self):
        self.counts = defaultdict(int)

    def intercept(self, method, request_or_iterator, call_details):
        """Call continuation and count status codes."""
        future = method(request_or_iterator, call_details)
        self.counts[future.code()] += 1
        return future


class AsyncCodeCountInterceptor(AsyncClientInterceptor):
    def __init__(self):
        self.counts = defaultdict(int)

    async def intercept(self, method, request_or_iterator, call_details):
        """Call continuation and count status codes."""
        future = await method(request_or_iterator, call_details)
        self.counts[await future.code()] += 1
        return future


class RetryInterceptor(ClientInterceptor):
    """Test interceptor that retries failed RPCs."""

    def __init__(self, retries):
        self._retries = retries

    def intercept(self, method, request_or_iterator, call_details):
        """Call the continuation and retry up to retries times if it fails."""
        tries_remaining = 1 + self._retries
        while 0 < tries_remaining:
            future = method(request_or_iterator, call_details)
            try:
                future.result()
                return future
            except Exception:
                tries_remaining -= 1

        return future


class AsyncRetryInterceptor(AsyncClientInterceptor):
    def __init__(self, retries):
        self._retries = retries

    async def intercept(self, method, request_or_iterator, call_details):
        """Call the continuation and retry up to retries times if it fails."""
        tries_remaining = 1 + self._retries
        while 0 < tries_remaining:
            future = await method(request_or_iterator, call_details)
            try:
                return await future
            except Exception:
                tries_remaining -= 1

        return future


class CrashingService:
    """Special case function that raises a given number of times before succeeding."""

    DEFAULT_EXCEPTION = ValueError("oops")

    def __init__(self, num_crashes, success_value="OK", exception=DEFAULT_EXCEPTION):
        self._num_crashes = num_crashes
        self._success_value = success_value
        self._exception = exception

    def __call__(self, *args, **kwargs):
        """Raise the first num_crashes times called, then return success_value."""
        if 0 < self._num_crashes:
            self._num_crashes -= 1
            raise self._exception

        return self._success_value


class CachingInterceptor(ClientInterceptor):
    """A test interceptor that caches responses based on input string."""

    def __init__(self):
        self._cache = {}

    def intercept(self, method, request_or_iterator, call_details):
        """Cache responses based on input string."""
        if hasattr(request_or_iterator, "__iter__"):
            request_or_iterator, copy_iterator = itertools.tee(request_or_iterator)
            cache_key = tuple(r.input for r in copy_iterator)
        else:
            cache_key = request_or_iterator.input

        if cache_key not in self._cache:
            self._cache[cache_key] = method(request_or_iterator, call_details)

        return self._cache[cache_key]


class AsyncCachingInterceptor(AsyncClientInterceptor):
    """A test interceptor that caches responses based on input string."""

    def __init__(self):
        self._cache = {}

    async def intercept(self, method, request_or_iterator, call_details):
        """Cache responses based on input string."""
        if hasattr(request_or_iterator, "__iter__"):
            request_or_iterator, copy_iterator = itertools.tee(request_or_iterator)
            cache_key = tuple(r.input for r in copy_iterator)
        else:
            cache_key = request_or_iterator.input

        if cache_key not in self._cache:
            self._cache[cache_key] = await method(request_or_iterator, call_details)

        return self._cache[cache_key]


@pytest.fixture
def metadata_string():
    """Expected joined metadata string."""
    return "this_key:this_value"


@pytest.mark.parametrize("aio", [False, True])
async def test_metadata_unary(metadata_string, aio):
    """Invocation metadata should be added to the servicer context."""
    intr = AsyncMetadataInterceptor([("this_key", "this_value")]) if aio else MetadataInterceptor(
        [("this_key", "this_value")])
    interceptors = [intr]

    special_cases = {
        "metadata": lambda _, c: ",".join(
            f"{key}:{value}" for key, value in c.invocation_metadata()
        )
    }
    with dummy_client(
            special_cases=special_cases, client_interceptors=interceptors, aio_server=aio, aio_client=aio,
    ) as client:
        if not aio:
            unary_output = client.Execute(DummyRequest(input="metadata")).output
        else:
            unary_output = await client.Execute(DummyRequest(input="metadata"))
            unary_output = unary_output.output
        assert metadata_string in unary_output


@pytest.mark.parametrize("aio", [False, True])
async def test_metadata_server_stream(metadata_string, aio):
    """Invocation metadata should be added to the servicer context."""
    intr = AsyncMetadataInterceptor([("this_key", "this_value")]) if aio else MetadataInterceptor(
        [("this_key", "this_value")])
    interceptors = [intr]

    special_cases = {
        "metadata": lambda _, c: ",".join(
            f"{key}:{value}" for key, value in c.invocation_metadata()
        )
    }
    with dummy_client(
            special_cases=special_cases, client_interceptors=interceptors, aio_server=aio, aio_client=aio,
    ) as client:
        if not aio:
            server_stream_output = [
                r.output
                for r in client.ExecuteServerStream(DummyRequest(input="metadata"))
            ]
        else:
            result = client.ExecuteServerStream(DummyRequest(input="metadata"))
            server_stream_output = [r.output async for r in result]
    assert metadata_string in "".join(server_stream_output)


@pytest.mark.parametrize("aio", [False, True])
async def test_metadata_client_stream(metadata_string, aio):
    """Invocation metadata should be added to the servicer context."""
    intr = AsyncMetadataInterceptor([("this_key", "this_value")]) if aio else MetadataInterceptor(
        [("this_key", "this_value")])
    interceptors = [intr]

    special_cases = {
        "metadata": lambda _, c: ",".join(
            f"{key}:{value}" for key, value in c.invocation_metadata()
        )
    }
    with dummy_client(
            special_cases=special_cases, client_interceptors=interceptors, aio_server=aio, aio_client=aio,
    ) as client:
        client_stream_input = iter((DummyRequest(input="metadata"),))
        if not aio:
            client_stream_output = client.ExecuteClientStream(
                client_stream_input
            ).output
        else:
            client_stream_output = await client.ExecuteClientStream(
                client_stream_input
            )
            client_stream_output = client_stream_output.output
    assert metadata_string in client_stream_output


@pytest.mark.parametrize("aio", [False, True])
async def test_metadata_client_server_stream(metadata_string, aio):
    """Invocation metadata should be added to the servicer context."""
    intr = AsyncMetadataInterceptor([("this_key", "this_value")]) if aio else MetadataInterceptor(
        [("this_key", "this_value")])
    interceptors = [intr]

    special_cases = {
        "metadata": lambda _, c: ",".join(
            f"{key}:{value}" for key, value in c.invocation_metadata()
        )
    }
    with dummy_client(
            special_cases=special_cases, client_interceptors=interceptors, aio_server=aio, aio_client=aio,
    ) as client:
        stream_stream_input = iter((DummyRequest(input="metadata"),))
        if not aio:
            result = client.ExecuteClientServerStream(stream_stream_input)
            stream_stream_output = [r.output for r in result]
        else:
            result = client.ExecuteClientServerStream(stream_stream_input)
            stream_stream_output = [r.output async for r in result]
    assert metadata_string in "".join(stream_stream_output)


@pytest.mark.parametrize("aio", [False, True])
async def test_code_counting(aio):
    """Access to code on call details works correctly."""
    interceptor = AsyncCodeCountInterceptor() if aio else CodeCountInterceptor()
    special_cases = {"error": raises(ValueError("oops"))}
    with dummy_client(
        special_cases=special_cases, client_interceptors=[interceptor], aio_server=aio, aio_client=aio,
    ) as client:
        assert interceptor.counts == {}
        if not aio:
            client.Execute(DummyRequest(input="foo"))
            assert interceptor.counts == {grpc.StatusCode.OK: 1}
            with pytest.raises(grpc.RpcError):
                client.Execute(DummyRequest(input="error"))
            assert interceptor.counts == {grpc.StatusCode.OK: 1, grpc.StatusCode.UNKNOWN: 1}
        else:
            await client.Execute(DummyRequest(input="foo"))
            assert interceptor.counts == {grpc.StatusCode.OK: 1}
            with pytest.raises(grpc.RpcError):
                await client.Execute(DummyRequest(input="error"))
            assert interceptor.counts == {grpc.StatusCode.OK: 1, grpc.StatusCode.UNKNOWN: 1}


@pytest.mark.parametrize("aio", [False, True])
async def test_basic_retry(aio):
    """Calling the continuation multiple times should work."""
    interceptor = AsyncRetryInterceptor(retries=1) if aio else RetryInterceptor(retries=1)
    special_cases = {"error_once": CrashingService(num_crashes=1)}
    with dummy_client(
        special_cases=special_cases, client_interceptors=[interceptor], aio_server=aio, aio_client=aio,
    ) as client:
        if not aio:
            assert client.Execute(DummyRequest(input="error_once")).output == "OK"
        else:
            result = await client.Execute(DummyRequest(input="error_once"))
            assert result.output == "OK"


@pytest.mark.parametrize("aio", [False, True])
async def test_failed_retry(aio):
    """The interceptor can return failed futures."""
    interceptor = AsyncRetryInterceptor(retries=1) if aio else RetryInterceptor(retries=1)
    special_cases = {"error_twice": CrashingService(num_crashes=2)}
    with dummy_client(
        special_cases=special_cases, client_interceptors=[interceptor], aio_server=aio, aio_client=aio,
    ) as client:
        if not aio:
            with pytest.raises(grpc.RpcError):
                client.Execute(DummyRequest(input="error_twice"))
        else:
            with pytest.raises(grpc.RpcError):
                await client.Execute(DummyRequest(input="error_twice"))


@pytest.mark.parametrize("aio", [False, True])
async def test_chaining(aio):
    """Chaining interceptors should work."""
    retry_interceptor = AsyncRetryInterceptor(retries=1) if aio else RetryInterceptor(retries=1)
    code_count_interceptor = AsyncCodeCountInterceptor() if aio else CodeCountInterceptor()
    interceptors = [retry_interceptor, code_count_interceptor]
    special_cases = {"error_once": CrashingService(num_crashes=1)}
    with dummy_client(
        special_cases=special_cases, client_interceptors=interceptors, aio_server=aio, aio_client=aio,
    ) as client:
        assert code_count_interceptor.counts == {}
        if not aio:
            assert client.Execute(DummyRequest(input="error_once")).output == "OK"
        else:
            result = await client.Execute(DummyRequest(input="error_once"))
            assert result.output == "OK"
        assert code_count_interceptor.counts == {
            grpc.StatusCode.OK: 1,
            grpc.StatusCode.UNKNOWN: 1,
        }


@pytest.mark.parametrize("aio", [False, True])
async def test_caching(aio):
    """Caching calls (not calling the continuation) should work."""
    caching_interceptor = AsyncCachingInterceptor() if aio else CachingInterceptor()
    # Use this to test how many times the continuation is called.
    code_count_interceptor = AsyncCodeCountInterceptor() if aio else CodeCountInterceptor()
    interceptors = [caching_interceptor, code_count_interceptor]
    with dummy_client(
            special_cases={}, client_interceptors=interceptors, aio_server=aio, aio_client=aio,
    ) as client:
        assert code_count_interceptor.counts == {}
        if not aio:
            assert client.Execute(DummyRequest(input="hello")).output == "hello"
            assert code_count_interceptor.counts == {grpc.StatusCode.OK: 1}
            assert client.Execute(DummyRequest(input="hello")).output == "hello"
            assert code_count_interceptor.counts == {grpc.StatusCode.OK: 1}
            assert client.Execute(DummyRequest(input="goodbye")).output == "goodbye"
            assert code_count_interceptor.counts == {grpc.StatusCode.OK: 2}
        else:
            result = await client.Execute(DummyRequest(input="hello"))
            assert result.output == "hello"
            assert code_count_interceptor.counts == {grpc.StatusCode.OK: 1}
            result = await client.Execute(DummyRequest(input="hello"))
            assert result.output == "hello"
            assert code_count_interceptor.counts == {grpc.StatusCode.OK: 1}
            result = await client.Execute(DummyRequest(input="goodbye"))
            assert result.output == "goodbye"
            assert code_count_interceptor.counts == {grpc.StatusCode.OK: 2}
        # Try streaming requests
        inputs = ["foo", "bar"]
        if not aio:
            input_iter = (DummyRequest(input=input) for input in inputs)
            assert client.ExecuteClientStream(input_iter).output == "foobar"
            assert code_count_interceptor.counts == {grpc.StatusCode.OK: 3}
            input_iter = (DummyRequest(input=input) for input in inputs)
            assert client.ExecuteClientStream(input_iter).output == "foobar"
            assert code_count_interceptor.counts == {grpc.StatusCode.OK: 3}
        else:
            input_iter = (DummyRequest(input=input) for input in inputs)
            result = await client.ExecuteClientStream(input_iter)
            assert result.output == "foobar"
            assert code_count_interceptor.counts == {grpc.StatusCode.OK: 3}
            input_iter = (DummyRequest(input=input) for input in inputs)
            result = await client.ExecuteClientStream(input_iter)
            assert result.output == "foobar"
            assert code_count_interceptor.counts == {grpc.StatusCode.OK: 3}
