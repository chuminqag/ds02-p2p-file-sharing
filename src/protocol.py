import json
import socket
import struct
import time


MAX_HEADER = 1024 * 1024
MAX_CHUNK = 1024 * 1024
IO_TIMEOUT = 5.0


class ProtocolError(ValueError):
    pass


def _remaining(sock, deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise socket.timeout('Frame deadline exceeded')
    sock.settimeout(remaining)


def recv_exact(sock, size, deadline):
    data = bytearray()
    while len(data) < size:
        _remaining(sock, deadline)
        block = sock.recv(min(size - len(data), 65536))
        if not block:
            raise ConnectionError('Connection closed before frame completed')
        data.extend(block)
    return bytes(data)


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolError('Duplicate JSON key')
        result[key] = value
    return result


def _constant(value):
    raise ProtocolError('Invalid JSON constant')


def recv_message(sock, max_payload=MAX_CHUNK, timeout=IO_TIMEOUT):
    previous = sock.gettimeout()
    deadline = time.monotonic() + timeout
    try:
        size = struct.unpack('!I', recv_exact(sock, 4, deadline))[0]
        if not 1 <= size <= MAX_HEADER:
            raise ProtocolError('Header size out of bounds')
        raw = recv_exact(sock, size, deadline)
        try:
            header = json.loads(raw.decode('utf-8'), object_pairs_hook=_object,
                                parse_constant=_constant)
        except (UnicodeError, ValueError, RecursionError) as exc:
            raise ProtocolError('Invalid JSON header') from exc
        if not isinstance(header, dict):
            raise ProtocolError('Header must be an object')
        size = header.get('payload_size')
        if type(size) is not int or not 0 <= size <= max_payload:
            raise ProtocolError('Payload size out of bounds')
        return header, recv_exact(sock, size, deadline)
    finally:
        sock.settimeout(previous)


def send_message(sock, header, payload=b'', timeout=IO_TIMEOUT):
    if not isinstance(header, dict) or not isinstance(payload, bytes):
        raise ProtocolError('Invalid outgoing message')
    if len(payload) > MAX_CHUNK:
        raise ProtocolError('Payload too large')
    frame = dict(header, payload_size=len(payload))
    try:
        raw = json.dumps(frame, ensure_ascii=True, allow_nan=False,
                         separators=(',', ':')).encode('utf-8')
    except (TypeError, ValueError, RecursionError) as exc:
        raise ProtocolError('Invalid outgoing JSON') from exc
    if len(raw) > MAX_HEADER:
        raise ProtocolError('Header too large')
    previous = sock.gettimeout()
    deadline = time.monotonic() + timeout
    try:
        _remaining(sock, deadline)
        sock.sendall(struct.pack('!I', len(raw)) + raw)
        if payload:
            _remaining(sock, deadline)
            sock.sendall(payload)
    finally:
        sock.settimeout(previous)


def request(host, port, header, timeout=IO_TIMEOUT):
    with socket.create_connection((host, port), timeout=timeout) as sock:
        send_message(sock, header, timeout=timeout)
        response, _ = recv_message(sock, max_payload=0, timeout=timeout)
        return response
