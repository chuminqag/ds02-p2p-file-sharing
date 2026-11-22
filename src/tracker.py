import hashlib
import json
import logging
import os
import socket
import threading
import time

from .protocol import MAX_CHUNK, ProtocolError, recv_message, send_message


MAX_CHUNKS = 4096
MAX_FILES = 256
MAX_PEERS = 128
MAX_CONNECTIONS = 32
MAX_FILE_SIZE = 256 * 1024 * 1024
LOG = logging.getLogger('tracker')


class TrackerError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def require(condition, message, code='INVALID_REQUEST'):
    if not condition:
        raise TrackerError(code, message)


def integer(value, lower, upper):
    return type(value) is int and lower <= value <= upper


def identifier(value):
    return (isinstance(value, str) and 1 <= len(value) <= 64
            and all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
                        '0123456789_-' for c in value))


def digest(value):
    return (isinstance(value, str) and len(value) == 64
            and all(c in '0123456789abcdef' for c in value))


def validate_metadata(data):
    fields = {'file_id', 'file_name', 'file_size', 'chunk_size',
              'chunk_count', 'chunk_hashes', 'file_hash', 'tracker'}
    require(isinstance(data, dict) and set(data) == fields, 'Invalid metadata fields')
    name = data['file_name']
    require(isinstance(name, str) and 1 <= len(name) <= 255
            and name not in ('.', '..') and not name.endswith((' ', '.'))
            and not any(ord(c) < 32 or c in '<>:"/\\|?*' for c in name),
            'Invalid file name')
    require(integer(data['file_size'], 0, MAX_FILE_SIZE), 'Invalid file size')
    require(integer(data['chunk_size'], 1, MAX_CHUNK), 'Invalid chunk size')
    count = (data['file_size'] + data['chunk_size'] - 1) // data['chunk_size']
    require(integer(data['chunk_count'], 0, MAX_CHUNKS)
            and data['chunk_count'] == count, 'Invalid chunk count')
    hashes = data['chunk_hashes']
    require(isinstance(hashes, list) and len(hashes) == count
            and all(digest(value) for value in hashes), 'Invalid chunk hashes')
    require(digest(data['file_hash']) and data['file_id'] == data['file_hash'],
            'File ID must equal the whole-file SHA-256')
    if not count:
        require(data['file_hash'] == hashlib.sha256(b'').hexdigest(),
                'Invalid empty-file hash')
    tracker = data['tracker']
    require(isinstance(tracker, dict) and set(tracker) == {'host', 'port'},
            'Invalid tracker endpoint')
    host = tracker['host']
    require(isinstance(host, str) and 1 <= len(host) <= 253
            and all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
                        '0123456789.-' for c in host), 'Invalid tracker host')
    require(integer(tracker['port'], 1, 65535), 'Invalid tracker port')
    return json.loads(json.dumps(data))


class Tracker:
    def __init__(self, host='127.0.0.1', port=9000, peer_keys=None,
                 lease_seconds=30.0, io_timeout=5.0):
        require(isinstance(peer_keys, dict) and 1 <= len(peer_keys) <= MAX_PEERS,
                'Configure DS02_PEER_KEYS with peer IDs and individual secrets')
        require(all(identifier(key) and isinstance(value, str)
                    and 16 <= len(value) <= 256 for key, value in peer_keys.items()),
                'Peer keys must have valid IDs and secrets of 16..256 characters')
        require(len(set(peer_keys.values())) == len(peer_keys), 'Use distinct peer secrets')
        require(integer(port, 0, 65535), 'Invalid port')
        require(0 < lease_seconds <= 3600 and 0 < io_timeout <= 60, 'Invalid timeout')
        self.host = host
        self.port = port
        self.lease_seconds = lease_seconds
        self.io_timeout = io_timeout
        self.key_hashes = {key: hashlib.sha256(value.encode()).digest()
                           for key, value in peer_keys.items()}
        self.files = {}
        self.peers = {}
        self.lock = threading.Lock()
        self.lifecycle = threading.Lock()
        self.stopping = threading.Event()
        self.ready = threading.Event()
        self.slots = threading.BoundedSemaphore(MAX_CONNECTIONS)
        self.clients = set()
        self.workers = set()
        self.listener = None

    def _authenticate(self, message):
        peer_id = message.get('peer_id')
        token = message.get('token')
        require(identifier(peer_id) and isinstance(token, str)
                and 16 <= len(token) <= 256, 'Authentication failed', 'UNAUTHORIZED')
        expected = self.key_hashes.get(peer_id, bytes(32))
        try:
            actual = hashlib.sha256(token.encode('utf-8')).digest()
        except UnicodeEncodeError:
            raise TrackerError('UNAUTHORIZED', 'Authentication failed') from None
        difference = 0
        for left, right in zip(expected, actual):
            difference |= left ^ right
        require(difference == 0 and peer_id in self.key_hashes,
                'Authentication failed', 'UNAUTHORIZED')
        return peer_id

    def _expire(self, now):
        expired = [key for key, peer in self.peers.items() if peer['expires'] <= now]
        for key in expired:
            del self.peers[key]
            LOG.info('[TRACKER] [LEASE_EXPIRED] [peer=%s removed]', key)

    def dispatch(self, message, address):
        require(message.get('version') == 1 and type(message.get('version')) is int,
                'Unsupported protocol version')
        require(identifier(message.get('request_id')), 'Invalid request ID')
        peer_id = self._authenticate(message)
        operation = message.get('op')
        require(operation in ('REGISTER_PEER', 'HEARTBEAT', 'REGISTER_FILE',
                              'GET_METADATA', 'ANNOUNCE', 'GET_PEERS', 'LEAVE'),
                'Unknown operation')
        now = time.monotonic()
        with self.lock:
            self._expire(now)
            if operation == 'REGISTER_PEER':
                port = message.get('port')
                session = message.get('session_id')
                require(integer(port, 1, 65535) and identifier(session),
                        'Invalid port or session ID')
                peer = self.peers.get(peer_id)
                if peer:
                    require(peer['session_id'] == session and peer['host'] == address
                            and peer['port'] == port, 'Active session already exists',
                            'SESSION_CONFLICT')
                    peer['expires'] = now + self.lease_seconds
                else:
                    self.peers[peer_id] = {'host': address, 'port': port,
                                          'session_id': session, 'files': {},
                                          'expires': now + self.lease_seconds}
                return {'lease_seconds': self.lease_seconds}
            peer = self.peers.get(peer_id)
            require(peer is not None, 'Register peer first', 'NOT_REGISTERED')
            require(message.get('session_id') == peer['session_id']
                    and peer['host'] == address, 'Session mismatch', 'SESSION_CONFLICT')
            if operation == 'HEARTBEAT':
                peer['expires'] = now + self.lease_seconds
                return {'lease_seconds': self.lease_seconds}
            if operation == 'LEAVE':
                del self.peers[peer_id]
                return {}
            if operation == 'REGISTER_FILE':
                metadata = validate_metadata(message.get('metadata'))
                file_id = metadata['file_id']
                existing = self.files.get(file_id)
                require(existing is None or existing == metadata,
                        'Metadata is immutable for this File ID', 'METADATA_CONFLICT')
                require(existing is not None or len(self.files) < MAX_FILES,
                        'File capacity reached', 'CAPACITY')
                self.files[file_id] = metadata
                return {'file_id': file_id}
            file_id = message.get('file_id')
            require(digest(file_id), 'Invalid File ID')
            metadata = self.files.get(file_id)
            require(metadata is not None, 'File not found', 'NOT_FOUND')
            if operation == 'GET_METADATA':
                return {'metadata': metadata}
            if operation == 'ANNOUNCE':
                chunks = message.get('chunks')
                revision = message.get('revision')
                require(integer(revision, 0, 2**53 - 1), 'Invalid revision')
                require(isinstance(chunks, list) and len(chunks) <= metadata['chunk_count']
                        and all(integer(i, 0, metadata['chunk_count'] - 1) for i in chunks)
                        and len(set(chunks)) == len(chunks), 'Invalid chunk indices')
                chunks = sorted(chunks)
                previous = peer['files'].get(file_id)
                if previous:
                    require(revision >= previous['revision'], 'Stale announcement', 'STALE_REVISION')
                    require(revision != previous['revision'] or chunks == previous['chunks'],
                            'Revision reused with different chunks', 'REVISION_CONFLICT')
                peer['files'][file_id] = {'revision': revision, 'chunks': chunks}
                return {'revision': revision}
            chunk = message.get('chunk_index')
            require(integer(chunk, 0, metadata['chunk_count'] - 1), 'Invalid chunk index')
            sources = [{'peer_id': key, 'host': value['host'], 'port': value['port']}
                       for key, value in sorted(self.peers.items())
                       if key != peer_id and chunk in value['files'].get(file_id, {}).get('chunks', [])]
            return {'chunk_index': chunk, 'peers': sources}

    def _handle(self, client, address):
        response = {'version': 1, 'request_id': None}
        event = 'REQUEST'
        try:
            message, _ = recv_message(client, max_payload=0, timeout=self.io_timeout)
            if identifier(message.get('request_id')):
                response['request_id'] = message['request_id']
            data = self.dispatch(message, address[0])
            event = message['op']
            response.update(ok=True, data=data)
        except TrackerError as exc:
            response.update(ok=False, error={'code': exc.code, 'message': str(exc)})
        except (ProtocolError, OSError, ConnectionError) as exc:
            response.update(ok=False, error={'code': 'BAD_FRAME', 'message': type(exc).__name__})
        except Exception:
            LOG.error('[TRACKER] [INTERNAL_ERROR] [request failed]')
            response.update(ok=False, error={'code': 'INTERNAL_ERROR', 'message': 'Request failed'})
        try:
            send_message(client, response, timeout=self.io_timeout)
        except (OSError, ConnectionError, ProtocolError):
            LOG.info('[TRACKER] [SEND] [connection unavailable]')
        finally:
            LOG.info('[TRACKER] [%s] [%s]', event,
                     'OK' if response['ok'] else response['error']['code'])
            client.close()
            with self.lifecycle:
                self.clients.discard(client)
                self.workers.discard(threading.current_thread())
            self.slots.release()

    def serve_forever(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
                    listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
                else:
                    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind((self.host, self.port))
                listener.listen(MAX_CONNECTIONS)
                listener.settimeout(0.2)
                self.listener = listener
                self.port = listener.getsockname()[1]
                self.ready.set()
                LOG.info('[TRACKER] [LISTEN] [%s:%s]', self.host, self.port)
                while not self.stopping.is_set():
                    with self.lock:
                        self._expire(time.monotonic())
                    try:
                        client, address = listener.accept()
                    except socket.timeout:
                        continue
                    except OSError:
                        if self.stopping.is_set():
                            break
                        raise
                    if not self.slots.acquire(blocking=False):
                        client.close()
                        LOG.info('[TRACKER] [ACCEPT] [BUSY]')
                        continue
                    worker = threading.Thread(target=self._handle, args=(client, address))
                    with self.lifecycle:
                        self.clients.add(client)
                        self.workers.add(worker)
                    try:
                        worker.start()
                    except RuntimeError:
                        client.close()
                        with self.lifecycle:
                            self.clients.discard(client)
                            self.workers.discard(worker)
                        self.slots.release()
                        raise
        finally:
            self.stop()
            with self.lifecycle:
                workers = list(self.workers)
            for worker in workers:
                worker.join(self.io_timeout + 1)
            LOG.info('[TRACKER] [STOP] [completed]')

    def stop(self):
        self.stopping.set()
        with self.lifecycle:
            clients = list(self.clients)
        for client in clients:
            try:
                client.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def main():
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    tracker = None
    try:
        keys = json.loads(os.environ.get('DS02_PEER_KEYS', '{}'))
        tracker = Tracker(host=os.environ.get('DS02_TRACKER_HOST', '127.0.0.1'),
                          port=int(os.environ.get('DS02_TRACKER_PORT', '9000')),
                          peer_keys=keys,
                          lease_seconds=float(os.environ.get('DS02_LEASE_SECONDS', '30')),
                          io_timeout=float(os.environ.get('DS02_IO_TIMEOUT', '5')))
        tracker.serve_forever()
    except KeyboardInterrupt:
        LOG.info('[TRACKER] [INTERRUPT] [Ctrl+C]')
    except (ValueError, OSError, RuntimeError):
        LOG.error('[TRACKER] [STARTUP] [invalid configuration or unavailable endpoint]')
        raise SystemExit(1)
    finally:
        if tracker:
            tracker.stop()


if __name__ == '__main__':
    main()
