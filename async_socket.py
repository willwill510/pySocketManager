'''
A rubust, higher level, pure python asynchronous socket handler including various helper functions, a custom protocol, integrated LZMA compression, hash verification, and multiple clientpool functionality.
'''

from typing import Callable, NoReturn, Optional, BinaryIO, Any
from json import dumps, loads
from os import path, remove
import hashlib
import asyncio
import lzma

DEFAULT_CLIENTPOOL = {}
FORMAT = {
    'bytes': b'b',
    'string': b's',
    'file': b'f',
}

def create_metadata(**kwargs) -> bytes:
    '''
    Creates JSON formatted and encoded bytes representing kwargs which can be sent as metadata through a socket connection.
    
    :param kwargs: Custom keyword arguments (E.g. type="chat", filename="picture.png", etc).
    :return: Returns JSON formatted and utf-8 encoded kwargs.
    :rtype: bytes
    '''
    return dumps(kwargs.copy()).encode()

def parse_metadata(data: bytes) -> dict:
    '''
    Parses valid metadata bytes into a dict
    
    :param data: A JSON formatted and utf-8 encoded dict object.
    :type data: bytes
    :return: A custom dict containing parsed metadata. 
    :rtype: dict[str, Any]
    '''
    return loads(data.decode())

async def start_server(host: str, port: int, *args, **kwargs) -> 'AsyncSocket':
    '''
    Initalize, start, and return a AsyncSocket object as a server.
    
    :param host: A valid internet protocol address used for connection and hosting.
    :type host: str
    :param port: A integer between 0 an 65535 used for connection and hosting.
    :type port: int
    :param args: Arguments which will be directly passed to the AsyncSocket object at initalization.
    :param kwargs: Keyword arguments which will be directly passed to the AsyncSocket object at initalization.
    :return: A AsyncSocket object configured to be a server and ready to be deployed.
    :rtype: AsyncSocket
    '''
    server = AsyncSocket(host, port, *args, **kwargs)
    await server.start()
    
    return server

class Sender():
    '''
    A wrapper for sending messages with the custom protocol used by AsyncSocket.
    '''

    def __init__(self, parent: 'AsyncSocket', source: str | bytes | BinaryIO, metadata: bytes, chunk_size: int = 1024, compression_level: int = 5, progress_callback: Optional[Callable] = None) -> None:
        '''
        A wrapper for sending messages with the custom protocol used by AsyncSocket.
        
        :param parent: The parent socket manager to send from.
        :type parent: 'AsyncSocket'
        :param source: The data to send through the socket connection, can be a string, bytes, or binary file object.
        :type source: str | bytes | BinaryIO
        :param metadata: Custom data describing your source which is sent with the header before the payload.
        :type metadata: bytes
        :param chunk_size: The chunk size used in reading, compressing, and hashing file objects. Default is 1024 bytes.
        :type chunk_size: int
        :param compression_level: the level of LZMA compression, minimum being 0 and maximum being 9. Default is 5
        :type compression_level: int
        :param progress_callback: An optional callback function which is called after sending a chunk of data and given the total size of the payload, and total number of bytes sent so far.
        :type progress_callback: Optional[Callable]
        '''
        self.parent = parent
        self.source = source
        self.metadata = metadata
        self.chunk_size = chunk_size
        self.compression_level = compression_level
        self.progress_callback = progress_callback
        self.header_sent = False
        self.done = False
        self.format = self._get_format()
        self.payload = self._create_payload()
        self.payload_size = self._get_payload_size()
        self.hash_digest = self._get_hash_digest()

    def _get_format(self) -> bytes:
        if isinstance(self.source, str):
            return FORMAT['string']
        elif isinstance(self.source, bytes):
            return FORMAT['bytes']
        elif hasattr(self.source, 'read') and hasattr(self.source, 'name'):
            return FORMAT['file']
        else:
            raise ValueError(f'Source is an invalid instance! ({type(self.source)})')
        
    def _create_compressed_file(self) -> BinaryIO:
        compressor = lzma.LZMACompressor(preset=self.compression_level)
        self.source.seek(0) # type: ignore
        with lzma.open(f'{self.source.name}.xz', 'wb', format=lzma.FORMAT_XZ) as file: # type: ignore
            while True:
                chunk = self.source.read(self.chunk_size) # type: ignore
                if not chunk:
                    break
                file.write(compressor.compress(chunk))
                file.flush()
            file.write(compressor.flush())

        return open(f'{self.source.name}.xz', 'rb') # type: ignore
    
    def _create_payload(self) -> bytes | BinaryIO:
        if self.format == FORMAT['string']:
            return lzma.compress(self.source.encode(), preset=self.compression_level) # type: ignore
        elif self.format == FORMAT['bytes']:
            return lzma.compress(self.source, preset=self.compression_level) # type: ignore
        elif self.format == FORMAT['file']:
            if not self.source.readable(): # type: ignore
                raise ValueError('BinaryIO is not readable!')
            return self._create_compressed_file()
        else:
            raise ValueError(f'Source is an invalid instance! ({type(self.source)})')
        
    def _get_payload_size(self) -> int:
        if self.format in (FORMAT['bytes'], FORMAT['string']):
            return len(self.payload) # type: ignore
        elif self.format == FORMAT['file']:
            return path.getsize(self.payload.name) # type: ignore
        else:
            raise ValueError(f'Payload is an invalid instance! ({type(self.payload)})')

    def _get_hash_digest(self) -> bytes:
        self.hasher = self.parent._hash()
        if self.format in (FORMAT['string'], FORMAT['bytes']):
            self.hasher.update(self.payload)
            return self.hasher.digest()
        elif self.format == FORMAT['file']:
            while True:
                chunk = self.payload.read(self.chunk_size) # type: ignore
                if not chunk:
                    break
                self.hasher.update(chunk)
            self.payload.seek(0) # type: ignore
            return self.hasher.digest()
        else:
            raise ValueError(f'Payload is an invalid instance! ({type(self.payload)})')

    async def _send_header(self) -> None:
        padded_metadata_size = len(self.metadata).to_bytes(self.parent.metadata_size_width, 'big')
        padded_payload_size = self.payload_size.to_bytes(self.parent.payload_size_width, 'big')
        encoded_compression_level = self.compression_level.to_bytes(byteorder='big')

        header = self.format + encoded_compression_level + padded_payload_size + self.hash_digest + padded_metadata_size + self.metadata
        self.parent._writer.write(header) # type: ignore
        await self.parent._writer.drain() # type: ignore

        self.header_sent = True

    async def _send_as_bytes(self) -> None:
        if self.done:
            raise RuntimeError('Redundent function call!')
        
        if not isinstance(self.payload, bytes):
            raise ValueError(f'Payload is an invalid instance! ({type(self.payload)})')
        
        total_bytes_sent = 0
        for index in range(0, self.payload_size, self.chunk_size):
            chunk = self.payload[index:index+self.chunk_size]
            
            self.parent._writer.write(chunk) # type: ignore
            await self.parent._writer.drain() # type: ignore
            
            total_bytes_sent += len(chunk)
            if self.progress_callback is not None:
                self.progress_callback(len(self.payload), total_bytes_sent)
        
        self.done = True

    async def _send_as_file(self) -> None:
        if self.done:
            raise RuntimeError('Redundent function call!')

        if not (hasattr(self.payload, "read") and hasattr(self.payload, "seek")):
            raise ValueError(f'Payload is an invalid instance! ({type(self.payload)})')
        
        total_bytes_sent = 0
        self.payload.seek(0) # type: ignore
        while total_bytes_sent < self.payload_size:
            chunk = self.payload.read(self.chunk_size) # type: ignore
            if not chunk:
                break
            
            self.parent._writer.write(chunk)  # type: ignore
            await self.parent._writer.drain()  # type: ignore
            
            total_bytes_sent += len(chunk)
            if self.progress_callback is not None:
                self.progress_callback(self.payload_size, total_bytes_sent)
        
        self.done = True
        self.payload.close() # type: ignore
        remove(self.payload.name) # type: ignore

    async def send(self) -> None:
        '''
        Sends data through the socket connection. The sender object may not be used after this method is called.

        :raises RuntimeError: If data has already been sent.
        '''
        if self.done:
            raise RuntimeError('Redundent function call!')
        
        await self._send_header()
        
        if self.format in (FORMAT['string'], FORMAT['bytes']):
            await self._send_as_bytes()
        elif self.format == FORMAT['file']:
            await self._send_as_file()
        else:
            raise ValueError(f'Source is an invalid instance! ({type(self.source)})')

class Receiver():
    '''
    A wrapper for receiving messages with the custom protocol used by AsyncSocket.
    '''

    def __init__(self, parent: 'AsyncSocket', chunk_size: int = 1024, progress_callback: Optional[Callable] = None) -> None:
        '''
        A wrapper for receiving messages with the custom protocol used by AsyncSocket.
        
        :param parent: The parent socket manager to receive from.
        :type parent: 'AsyncSocket'
        :param chunk_size: The chunk size used in reading, compressing, and hashing file objects. Default is 1024 bytes.
        :type chunk_size: int
        :param progress_callback: An optional callback function which is called after receiving a chunk of bytes and given the total size of the payload, and total number of bytes received so far.
        :type progress_callback: Optional[Callable]
        '''
        self.parent = parent
        self.chunk_size = chunk_size
        self.progress_callback = progress_callback
        self.header_recved = False
        self.done = False

    async def recv_header(self) -> None:
        '''
        Receive a header from the data stream and create the following attributes:
        * format
        * compression_level
        * payload_size
        * hash_digest
        * metadata
        * metadata_size
        '''
        if self.done or self.header_recved:
            raise RuntimeError('Redundent function call!')

        self.format = await self.parent._reader.readexactly(1) # type: ignore
        self.compression_level = int.from_bytes(await self.parent._reader.readexactly(1)) # type: ignore
        self.payload_size = int.from_bytes(await self.parent._reader.readexactly(self.parent.payload_size_width), 'big') # type: ignore
        self.hash_digest = await self.parent._reader.readexactly(self.parent._hash().digest_size) # type: ignore
        self.metadata_size = int.from_bytes(await self.parent._reader.readexactly(self.parent.metadata_size_width), 'big') # type: ignore
        self.metadata = await self.parent._reader.readexactly(self.metadata_size) # type: ignore
        
        self.header_recved = True
    
    async def _recv_as_bytes(self) -> bytes:
        if self.done:
            raise RuntimeError('Redundent function call!')

        payload = bytearray()
        while len(payload) < self.payload_size:
            chunk = await self.parent._reader.readexactly(min(self.chunk_size, self.payload_size - len(payload))) # type: ignore
            payload += chunk

            if self.progress_callback is not None:
                self.progress_callback(self.payload_size, len(payload)) # type: ignore

        data = lzma.decompress(payload)
        hasher = self.parent._hash()
        hasher.update(payload)
        if hasher.digest() != self.hash_digest:
            raise ConnectionError('Data hash does not match expected!')
        
        self.done = True
        return data
    
    async def _recv_as_file(self, file: BinaryIO) -> None:
        if self.done:
            raise RuntimeError('Redundent function call!')

        if not file.writable():
            raise PermissionError('File is not writable!')

        decompressor = lzma.LZMADecompressor()
        hasher = self.parent._hash()
        bytes_recved = 0
        file_size = 0
        while bytes_recved < self.payload_size:
            chunk = await self.parent._reader.readexactly(min(self.chunk_size, self.payload_size - bytes_recved)) # type: ignore
            bytes_recved += len(chunk)
            hasher.update(chunk)
            decompressed_chunk = decompressor.decompress(chunk)
            file_size += file.write(decompressed_chunk)
            file.flush()

        if file_size != self.payload_size:
            raise EnvironmentError('Bytes lost while writing to file!')
        
        if hasher.digest() != self.hash_digest:
            raise ConnectionError('Data hash does not match expected!')
        
        self.done = True
        
    async def recv(self, file: Optional[BinaryIO] = None) -> tuple[bytes, bytes | str | None]:
        '''
        Receives data through the socket connection. The receiver object may not be used after this method is called.
        
        :param file: A binary file object to write to if the message is in a file format.
        :type file: Optional[BinaryIO]
        :return: Tuple with the received metadata followed by the message content in bytes or string form, returns None if the content is written to a file.
        :rtype: tuple[bytes, bytes | str | None]
        :raises RuntimeError: If data has already been received.
        :raises ValueError: If there was no file given and the message is in a file format.
        :raises EnviromentError: If an invalid format is detected.
        '''
        if self.done:
            raise RuntimeError('Redundent function call!')

        if not self.header_recved:
            await self.recv_header()

        if self.format == FORMAT['bytes']:
            payload = await self._recv_as_bytes()
        elif self.format == FORMAT['string']:
            payload = (await self._recv_as_bytes()).decode()
        elif self.format == FORMAT['file']:
            if not file:
                raise ValueError('file not provided')

            payload = await self._recv_as_file(file)
        else:
            raise EnvironmentError(f'Invalid format! ({self.format})')

        return self.metadata, payload

class Request():
    
    def __init__(self, func: Callable, args: tuple = (), kwargs: dict = {}) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.done = False
        self.result = None

    async def run(self) -> Any:
        '''
        Calls the given function asynchronously without waiting for a return.
        
        :return: Returns the result of the given function.
        :rtype: Any
        '''
        if asyncio.iscoroutinefunction(self.func):
            self.result = await self.func(*self.args, **self.kwargs)
        else:
            self.result = await asyncio.to_thread(self.func, *self.args, **self.kwargs)
        self.done = True
        return self.result
    
    async def wait_for(self, timeout: Optional[float] = None) -> Any:
        '''
        Wait for given function to finish running and return it's result. Note that this moethod does not call the function it only waits for a return.
        
        :param timeout: A timeout in seconds which will raise a TimeoutError if the wait is longer than specified. Set to None for no timeout. Default is None.
        :type timeout: Optional[float]
        :return: Returns the result of the given function.
        :rtype: Any
        '''
        start_time = asyncio.get_event_loop().time()
        while not self.done:
            if timeout is not None and (asyncio.get_event_loop().time() - start_time) > timeout:
                raise TimeoutError('Request timed out')
            await asyncio.sleep(0.01)
        return self.result

class AsyncSocket():
    '''
    A structured interface for interacting with sockets asynchronously.
    '''

    def __init__(self, host: str, port: int, label: str = '', metadata_size_width: int = 8, payload_size_width: int = 8, hash_func: Callable = hashlib.sha256, send_request_queue: Optional[asyncio.Queue] = None, recv_request_queue: Optional[asyncio.Queue] = None, reader: Optional[asyncio.StreamReader] = None, writer: Optional[asyncio.StreamWriter] = None) -> None:
        '''
        A structured interface for interacting with sockets asynchronously.
        
        :param host: A valid internet protocol address used for connection and hosting.
        :type host: str
        :param port: A integer between 0 an 65535 used for connection and hosting.
        :type port: int
        :param label: An optional label which is sent when connecting to another socket for clarity while managing several connections. Default is an empty string.
        :type label: str
        :param metadata_size_width: The width of the metadata size in the header, this determines the max length of a massage's metadata and in almost all cases the default is fine. Default is 8.
        :type metadata_size_width: int
        :param payload_size_width: The width of the payload size in the header, this determines the max length of a message and in almost all cases the default is fine. Default is 8.
        :type payload_size_width: int
        :param hash_func: The hash function to use when verifying message content, this must be one of the hash functions from the hashlib library and in almost all cases the default is fine. Default is sha256.
        :type hash_func: Callable
        :param send_request_queue: An asyncio queue which is used to queue send requests in an organized way, useful for merging queues of multiple AsyncSockets. If set to None a new asyncio queue will be initialized. Default is None.
        :type send_request_queue: Optional[asyncio.Queue]
        :param recv_request_queue: An asyncio queue which is used to queue receive requests in an organized way, useful for merging queues of multiple AsyncSockets. If set to None a new asyncio queue will be initialized. Default is None.
        :type recv_request_queue: Optional[asyncio.Queue]
        :param reader: An asyncio stream reader which is used to asynchronously receive data through a socket connection. If set to None a new asyncio stream reader will be initalized. This attribute almost never needs to be set manually. Default is None
        :type reader: Optional[asyncio.StreamReader]
        :param writer: An asyncio stream writer which is used to asynchronously send data through a socket connection. If set to None a new asyncio stream writer will be initalized. This attribute almost never needs to be set manually. Default is None
        :type writer: Optional[asyncio.StreamWriter]
        '''
        self.default_clientpool: dict[str, 'AsyncSocket'] = {}
        self.host = host
        self.port = port
        self.label = label
        self.metadata_size_width = metadata_size_width
        self.payload_size_width = payload_size_width
        self._hash = hash_func
        self._reader = reader
        self._writer = writer
        self._send_request_queue = send_request_queue if send_request_queue else asyncio.Queue()
        self._recv_request_queue = recv_request_queue if recv_request_queue else asyncio.Queue()
        self._send_request_handler = asyncio.create_task(self._send_request_handler_loop())
        self._recv_request_handler = asyncio.create_task(self._recv_request_handler_loop())
        self._server: Optional[asyncio.AbstractServer] = None
        self._accept_queue: Optional[asyncio.Queue] = None

    async def connect(self, metadata: bytes, timeout: Optional[float] = None) -> str:
        '''
        Connects to a server and sends an initial message with metadata and the label.
        
        :param metadata: Custom data describing your data which is sent with the header before the payload.
        :type metadata: bytes
        :param timeout: A timeout in seconds which will raise a TimeoutError if the wait is longer than specified. Set to None for no timeout. Default is None.
        :type timeout: Optional[float]
        :return: Returns the connected socket's label.
        :rtype: str
        '''
        self._reader, self._writer = await asyncio.wait_for(asyncio.open_connection(self.host, self.port), timeout)
        await self.send(metadata, self.label.encode(), timeout)
        return (await self.expect(metadata, timeout)).decode() # type: ignore

    async def send(self, metadata: bytes, data: bytes | str, timeout: Optional[float] = None, chunk_size: int = 1024, compression_level = 5, progress_callback: Optional[Callable] = None) -> None:
        '''
        Sends data through the socket connection.
        
        :param metadata: Custom data describing your data which is sent with the header before the payload.
        :type metadata: bytes
        :param data: The data to send through the socket connection, can be a string, bytes, or binary file object.
        :type data: bytes | str
        :param timeout: A timeout in seconds which will raise a TimeoutError if the wait is longer than specified. Set to None for no timeout. Default is None.
        :type timeout: Optional[float]
        :param chunk_size: The chunk size used in reading, compressing, and hashing file objects. Default is 1024 bytes.
        :type chunk_size: int
        :param compression_level: the level of LZMA compression, minimum being 0 and maximum being 9. Default is 5.
        :type compression_level: int
        :param progress_callback: An optional callback function which is called after sending a chunk of data and given the total size of the payload, and total number of bytes sent so far.
        :type progress_callback: Optional[Callable]
        :raises ConnectionError: If the AsyncSocket is not connected to anything.
        '''
        if self._writer is None or self._reader is None:
            raise ConnectionError('Not connected')

        sender = Sender(self, data, metadata, chunk_size, compression_level, progress_callback)
        request = Request(sender.send)
        
        self._send_request_queue.put_nowait(request)
        await request.wait_for(timeout)

    async def recv(self, timeout: Optional[float] = None, chunk_size: int = 1024, progress_callback: Optional[Callable] = None) -> tuple[bytes, bytes | str | None]:
        '''
        Receive data through socket connection.

        :param timeout: A timeout in seconds which will raise a TimeoutError if the wait is longer than specified. Set to None for no timeout. Default is None.
        :type timeout: Optional[float]
        :param chunk_size: The chunk size used in reading, compressing, and hashing file objects. Default is 1024 bytes.
        :type chunk_size: int
        :param progress_callback: An optional callback function which is called after receiving a chunk of bytes and given the total size of the payload, and total number of bytes received so far.
        :type progress_callback: Optional[Callable]
        :return: Tuple with the received metadata followed by the message content in bytes or string form, returns None if the content is written to a file.
        :rtype: tuple[bytes, bytes | str | None]
        '''
        if self._reader is None or self._writer is None:
            raise ConnectionError('Not connected')

        message = Receiver(self, chunk_size, progress_callback)
        await message.recv_header()

        request = Request(message.recv)
        self._recv_request_queue.put_nowait(request) # type: ignore
        return await request.wait_for(timeout)

    async def expect(self, expected_metadata: bytes, timeout: Optional[float] = None) -> bytes | str | None:
        '''
        Receive data through a socket connection and compare the metadata bytes to expected metadata bytes, raising an error if they don't match.
        
        :param expected_metadata: The metadata bytes to compare to the received metadata bytes.
        :type expected_metadata: bytes
        :param timeout: A timeout in seconds which will raise a TimeoutError if the wait is longer than specified. Set to None for no timeout. Default is None.
        :type timeout: Optional[float]
        :return: Returns the message content in bytes or string form, returns None if the content is written to a file.
        :rtype: bytes | str | None
        :raises ValueError: If the metadata received did not match the expected metadata
        '''
        metadata, data = await self.recv(timeout)
        if metadata == expected_metadata:
            return data
        
        raise ValueError('Unexpected value!')

    async def start(self, backlog: int = 0) -> None:
        '''
        Start the server and begin accepting clients.
        
        :param backlog: The maximum number of queued connections before refusing new incoming connections.
        :type backlog: int
        '''
        if self._accept_queue is None:
            self._accept_queue = asyncio.Queue()

        async def _on_connect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            peer = writer.get_extra_info('peername') or (self.host, self.port)
            client = AsyncSocket(peer[0], peer[1], send_request_queue=self._send_request_queue, recv_request_queue=self._recv_request_queue, reader=reader, writer=writer)

            await self._accept_queue.put(client) # type: ignore

        self._server = await asyncio.start_server(_on_connect, self.host, self.port, backlog=backlog)

    async def close(self) -> None:
        '''
        Closes existing connections from the server and stop serving. The AsyncSocket may not be used after this method is called.

        :raises RuntimeError: If the AsyncSocket is not connected to anything.
        '''
        if self._server is None:
            raise RuntimeError('Server not started! (call start())')

        self._server.close()
        await self._server.wait_closed()
        self._server.close_clients()
        self._reader = None
        self._writer = None
        self._server = None
        self._accept_queue = None

    async def accept(self, metadata: bytes, clientpool: dict[str, 'AsyncSocket'] = DEFAULT_CLIENTPOOL, timeout: Optional[float] = None) -> 'AsyncSocket':
        '''
        Accepts a client connection and optionally enters it into a specified clientpool.
        
        :param metadata: Custom data describing your data which is sent with the header before the payload.
        :type metadata: bytes
        :param clientpool: A pool of several clients grouped together in a dictionary where each key is the client's label and the value is the client's AsyncSocket object. When accepted, the client will be put into the supplied clientpool. Default is DEFAULT_CLIENTPOOL.
        :type clientpool: Optional[Dict[str, 'AsyncSocket']]
        :param timeout: A timeout in seconds which will raise a TimeoutError if the wait is longer than specified. Set to None for no timeout. Default is None.
        :type timeout: Optional[float]
        :return: Returns the AsyncSocket associated with the accepted client.
        :rtype: AsyncSocket
        :raises RuntimeError: If AsyncSocket object has not been started as a server.
        :raises ConnectionError: If the connection handshake fails.
        '''
        if self._accept_queue is None:
            raise RuntimeError('Server not started! (call start())')

        client: AsyncSocket = await asyncio.wait_for(self._accept_queue.get(), timeout)

        try:
            client.label = f'{(await client.expect(metadata))}:{client.port}'
            await client.send(metadata, self.label.encode())
        except (ValueError, TimeoutError, OverflowError, MemoryError):
            if client._writer:
                client._writer.close()
                await client._writer.wait_closed()
            
            raise ConnectionError('Handshake failed!')

        if clientpool is not None:
            clientpool[client.label] = client
        else:
            self.default_clientpool[client.label] = client

        return client

    async def broadcast(self, metadata: bytes, data: bytes, clientpool: dict[str, 'AsyncSocket'] = DEFAULT_CLIENTPOOL, timeout: float = 4) -> None:
        '''
        Broadcast a message to every client in a given clientpool.
        
        :param metadata: Custom data describing your data which is sent with the header before the payload.
        :type metadata: bytes
        :param data: The data to send through the socket connection, can be a string, bytes, or binary file object.
        :type data: bytes
        :param clientpool: A pool of several clients grouped together in a dictionary where each key is the client's label and the value is the client's AsyncSocket object, which will be looped through, sending the same message to each. Default is DEFAULT_CLIENTPOOL.
        :type clientpool: Dict[str, 'AsyncSocket']
        :param timeout: A timeout in seconds which will raise a TimeoutError if the wait is longer than specified. Set to None for no timeout. Default is 4.
        :type timeout: float
        '''
        clients = list(clientpool.copy().values())
        for client in clients:
            if client is None:
                continue
            
            try:
                await client.send(metadata, data, timeout)
            except (ConnectionError, asyncio.IncompleteReadError, asyncio.TimeoutError):
                clientpool.pop(client.label, None)
                if client._writer:
                    client._writer.close()
                    await client._writer.wait_closed()

    async def _send_request_handler_loop(self) -> NoReturn:
        while True:
            request: Request = await self._send_request_queue.get() # type: ignore
            await request.run()
    
    async def _recv_request_handler_loop(self) -> NoReturn:
        while True:
            request: Request = await self._recv_request_queue.get() # type: ignore
            await request.run()

    async def accept_loop(self, metadata: bytes, clientpool: dict[str, 'AsyncSocket'] = DEFAULT_CLIENTPOOL, timeout: Optional[float] = None) -> NoReturn:
        '''
        A loop which constantly accepts new connections to a clientpool, meant to be used in a seperate task from the main event loop.
        
        :param metadata: Custom data describing your data which is sent with the header before the payload.
        :type metadata: bytes
        :param clientpool: A pool of several clients grouped together in a dictionary where each key is the client's label and the value is the client's AsyncSocket object. When accepted the client will be put into the supplied clientpool. Default is DEFAULT_CLIENTPOOL.
        :type clientpool: Dict[str, 'AsyncSocket']
        :param timeout: A timeout in seconds which will raise a TimeoutError if the wait is longer than specified. Set to None for no timeout. Default is None.
        :type timeout: Optional[float]
        '''
        while True:
            try:
                await self.accept(metadata, clientpool, timeout)
            except asyncio.TimeoutError:
                continue
            except Exception:
                continue

    async def ping_loop(self, metadata: bytes, clientpool: dict[str, 'AsyncSocket'] = DEFAULT_CLIENTPOOL, interval: int = 30, data: bytes = b'ping') -> NoReturn:
        '''
        Docstring for ping_loop
        
        :param metadata: Custom data describing your data which is sent with the header before the payload.
        :type metadata: bytes
        :param clientpool: A pool of several clients grouped together in a dictionary where each key is the client's label and the value is the client's AsyncSocket object, which will be looped through, pinging each to check connection. Default is DEFAULT_CLIENTPOOL.
        :type clientpool: Dict[str, 'AsyncSocket']
        :param interval: The interval in seconds between pings. Default is 30.
        :type interval: int
        :param data: The dummy data to send through the socket connection as a ping. Default is "ping".
        :type data: bytes
        '''
        while True:
            await self.broadcast(metadata, data, clientpool)
            await asyncio.sleep(interval)

    def is_running(self) -> bool:
        '''
        Checks if this AsyncSocket is running as a server.
        
        :return: The state of the server.
        :rtype: bool
        '''
        return self._server is not None