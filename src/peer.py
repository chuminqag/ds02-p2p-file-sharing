import hashlib
import json
import logging
import os
import socket
import threading
import time

from .protocol import MAX_CHUNK, ProtocolError, recv_message, request, send_message
from .tracker import MAX_FILE_SIZE, digest, identifier, integer, validate_metadata


LOG = logging.getLogger('peer')
NETWORK_ERRORS = (OSError, ConnectionError, ValueError)


class PeerError(ValueError):
    pass


def check(condition, message):
    if not condition:
        raise PeerError(message)


def same_secret(left, right):
    if not isinstance(left, str) or not 16 <= len(left) <= 256:
        return False
    difference = 0
    for a, b in zip(hashlib.sha256(left.encode()).digest(), right):
        difference |= a ^ b
    return difference == 0


class Peer:
    def __init__(self, peer_id, token, swarm_key, port, storage,
                 tracker_host='127.0.0.1', tracker_port=9000, host='127.0.0.1',
                 workers=4, timeout=3.0, retries=5, upload_delay=0.0,
                 corrupt_index=-1):
        check(identifier(peer_id), 'Invalid peer ID')
        check(all(isinstance(s, str) and 16 <= len(s) <= 256
                  for s in (token, swarm_key)), 'Invalid credentials')
        check(integer(port, 0, 65535) and integer(tracker_port, 1, 65535), 'Invalid port')
        check(integer(workers, 1, 16) and integer(retries, 1, 20), 'Invalid worker/retry limit')
        check(0 < timeout <= 60 and 0 <= upload_delay <= 30, 'Invalid timeout/delay')
        check(integer(corrupt_index, -1, 4095), 'Invalid fault index')
        self.peer_id = peer_id
        self.token = token
        self.swarm_key = swarm_key
        self.swarm_hash = hashlib.sha256(swarm_key.encode()).digest()
        self.host, self.port = host, port
        self.tracker_host, self.tracker_port = tracker_host, tracker_port
        self.storage = os.path.realpath(storage)
        os.makedirs(self.storage, exist_ok=True)
        self.workers, self.timeout, self.retries = workers, timeout, retries
        self.upload_delay, self.corrupt_index = upload_delay, corrupt_index
        self.session = os.urandom(16).hex()
        self.files = {}
        self.lock = threading.Lock()
        self.sync_lock = threading.Lock()
        self.operation_lock = threading.Lock()
        self.lifecycle = threading.Lock()
        self.stopping = threading.Event()
        self.upload_slots = threading.BoundedSemaphore(16)
        self.sockets = set()
        self.upload_threads = set()
        self.background = []
        self.listener = None
        self.lease = 30.0
        self.storage_lock = None

    def log(self, event, result):
        LOG.info('[%s] [%s] [%s]', self.peer_id, event, result)

    def _path(self, file_id, name):
        check(digest(file_id), 'Invalid file ID')
        path = os.path.realpath(os.path.join(self.storage, file_id, name))
        check(os.path.commonpath((self.storage, path)) == self.storage, 'Unsafe storage path')
        return path

    def _rpc(self, op, **fields):
        message = dict(version=1, request_id=os.urandom(16).hex(), op=op,
                       peer_id=self.peer_id, token=self.token, session_id=self.session, **fields)
        for attempt in range(3):
            try:
                if op == 'LEAVE':
                    response = request(self.tracker_host, self.tracker_port, message,
                                       timeout=min(self.timeout, 0.3))
                else:
                    address = (self.tracker_host, self.tracker_port)
                    response, _ = self._exchange(address, message, max_payload=0)
                check(integer(response.get('version'), 1, 1)
                      and response.get('request_id') == message['request_id'],
                      'Invalid tracker response')
                if response.get('ok') is not True:
                    raise PeerError('Tracker: ' + str(response.get('error', {}).get('code', 'ERROR')))
                check(isinstance(response.get('data'), dict), 'Invalid tracker data')
                return response['data']
            except (OSError, ConnectionError):
                if attempt == 2 or self.stopping.wait(0.1 * (attempt + 1)):
                    raise

    def _exchange(self, address, message, max_payload):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            with self.lifecycle:
                check(not self.stopping.is_set(), 'Peer stopping')
                self.sockets.add(sock)
            try:
                sock.settimeout(self.timeout)
                sock.connect(address)
                send_message(sock, message, timeout=self.timeout)
                return recv_message(sock, max_payload, self.timeout)
            finally:
                with self.lifecycle:
                    self.sockets.discard(sock)

    def _sync(self):
        with self.sync_lock:
            data = self._rpc('REGISTER_PEER', port=self.port)
            lease = data.get('lease_seconds')
            check(isinstance(lease, (int, float)) and 0 < lease <= 3600, 'Invalid lease')
            self.lease = lease
            with self.lock:
                snapshots = [(state['metadata'], state['revision'], sorted(state['chunks']))
                             for state in self.files.values()]
            for metadata, revision, chunks in snapshots:
                self._rpc('REGISTER_FILE', metadata=metadata)
                self._rpc('ANNOUNCE', file_id=metadata['file_id'], revision=revision, chunks=chunks)

    def _heartbeat(self):
        while not self.stopping.wait(min(1.0, self.lease / 3)):
            try:
                self._sync()
            except NETWORK_ERRORS as exc:
                self.log('TRACKER_RETRY', type(exc).__name__)

    def start(self):
        check(self.listener is None, 'Peer already started')
        lock_path = os.path.join(self.storage, '.active')
        try:
            self.storage_lock = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(self.storage_lock, str(os.getpid()).encode())
            self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
                self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            self.listener.bind((self.host, self.port))
            self.listener.listen(16)
            self.listener.settimeout(0.2)
            self.port = self.listener.getsockname()[1]
            server = threading.Thread(target=self._serve, name=self.peer_id + '-upload')
            server.start()
            self.background.append(server)
            self._sync()
            heartbeat = threading.Thread(target=self._heartbeat, name=self.peer_id + '-heartbeat')
            heartbeat.start()
            self.background.append(heartbeat)
            self.log('LISTEN', f'{self.host}:{self.port}')
        except BaseException:
            self.stop()
            raise

    def _serve(self):
        while not self.stopping.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self.stopping.is_set():
                    self.log('ACCEPT', 'FAILED')
                break
            if not self.upload_slots.acquire(blocking=False):
                client.close()
                continue
            worker = threading.Thread(target=self._upload, args=(client,))
            with self.lifecycle:
                if self.stopping.is_set():
                    client.close()
                    self.upload_slots.release()
                    break
                self.sockets.add(client)
                self.upload_threads.add(worker)
            try:
                worker.start()
            except RuntimeError:
                with self.lifecycle:
                    self.sockets.discard(client)
                    self.upload_threads.discard(worker)
                client.close()
                self.upload_slots.release()
                self.log('UPLOAD', 'WORKER_START_FAILED')

    def _upload(self, client):
        request_id = None
        try:
            header, _ = recv_message(client, max_payload=0, timeout=self.timeout)
            check(identifier(header.get('request_id')), 'Invalid request ID')
            request_id = header['request_id']
            check(integer(header.get('version'), 1, 1) and header.get('op') == 'GET_CHUNK',
                  'Invalid operation')
            check(identifier(header.get('peer_id'))
                  and same_secret(header.get('swarm_key'), self.swarm_hash), 'Unauthorized')
            file_id, index = header.get('file_id'), header.get('chunk_index')
            check(digest(file_id) and integer(index, 0, 4095), 'Invalid chunk request')
            with self.lock:
                state = self.files.get(file_id)
                check(state is not None and index in state['chunks'], 'Chunk unavailable')
                metadata = state['metadata']
                complete = len(state['chunks']) == metadata['chunk_count']
            with open(self._path(file_id, f'{index}.chunk'), 'rb') as stream:
                payload = stream.read(metadata['chunk_size'] + 1)
            check(self._valid_chunk(metadata, index, payload), 'Local chunk corrupt')
            if self.stopping.wait(self.upload_delay):
                return
            if index == self.corrupt_index:
                payload = bytes([payload[0] ^ 1]) + payload[1:]
                self.log('FAULT_INJECT', f'chunk={index} corrupt')
            send_message(client, dict(version=1, request_id=request_id, ok=True,
                                      file_id=file_id, chunk_index=index), payload, self.timeout)
            self.log('UPLOAD', f'chunk={index} to={header["peer_id"]} complete={complete} OK')
        except NETWORK_ERRORS as exc:
            self.log('UPLOAD_REJECT', type(exc).__name__)
            try:
                send_message(client, dict(version=1, request_id=request_id, ok=False,
                                          error='CHUNK_UNAVAILABLE'), timeout=self.timeout)
            except NETWORK_ERRORS:
                pass
        finally:
            with self.lifecycle:
                self.sockets.discard(client)
                self.upload_threads.discard(threading.current_thread())
            client.close()
            self.upload_slots.release()

    def _valid_chunk(self, metadata, index, payload):
        expected_size = min(metadata['chunk_size'],
                            metadata['file_size'] - index * metadata['chunk_size'])
        return (len(payload) == expected_size
                and hashlib.sha256(payload).hexdigest() == metadata['chunk_hashes'][index])

    def _prepare(self, metadata):
        metadata = validate_metadata(metadata)
        file_id = metadata['file_id']
        with self.lock:
            existing = self.files.get(file_id)
            if existing:
                check(existing['metadata'] == metadata, 'Metadata conflict')
                return
        directory = os.path.dirname(self._path(file_id, 'metadata.json'))
        os.makedirs(directory, exist_ok=True)
        chunks = set()
        for index in range(metadata['chunk_count']):
            try:
                with open(self._path(file_id, f'{index}.chunk'), 'rb') as stream:
                    payload = stream.read(metadata['chunk_size'] + 1)
                if self._valid_chunk(metadata, index, payload):
                    chunks.add(index)
            except FileNotFoundError:
                pass
        temporary = self._path(file_id, 'metadata.tmp')
        with open(temporary, 'w', encoding='utf-8') as stream:
            json.dump(metadata, stream)
        os.replace(temporary, self._path(file_id, 'metadata.json'))
        with self.lock:
            self.files[file_id] = dict(metadata=metadata, chunks=chunks, revision=0)
        self.log('RESUME_SCAN', f'verified={len(chunks)}/{metadata["chunk_count"]}')

    def _store(self, metadata, index, payload):
        check(self._valid_chunk(metadata, index, payload), 'Chunk hash mismatch')
        file_id = metadata['file_id']
        temporary = self._path(file_id, f'{index}.part')
        with open(temporary, 'wb') as stream:
            stream.write(payload)
        os.replace(temporary, self._path(file_id, f'{index}.chunk'))
        with self.lock:
            state = self.files[file_id]
            state['chunks'].add(index)
            state['revision'] += 1

    def seed(self, path, chunk_size=65536):
        with self.operation_lock:
            check(integer(chunk_size, 1, MAX_CHUNK), 'Invalid chunk size')
            whole_hash = hashlib.sha256()
            hashes, size = [], 0
            with open(path, 'rb') as stream:
                while True:
                    payload = stream.read(chunk_size)
                    if not payload:
                        break
                    size += len(payload)
                    check(size <= MAX_FILE_SIZE and len(hashes) < 4096, 'File too large')
                    whole_hash.update(payload)
                    hashes.append(hashlib.sha256(payload).hexdigest())
            file_id = whole_hash.hexdigest()
            metadata = dict(file_id=file_id, file_name=os.path.basename(path), file_size=size,
                            chunk_size=chunk_size, chunk_count=len(hashes), chunk_hashes=hashes,
                            file_hash=file_id, tracker=dict(host=self.tracker_host, port=self.tracker_port))
            self._prepare(metadata)
            with open(path, 'rb') as stream:
                for index in range(len(hashes)):
                    check(not self.stopping.is_set(), 'Stopped')
                    self._store(metadata, index, stream.read(chunk_size))
                check(not stream.read(1), 'Source changed during seeding')
            self._sync()
            self.log('SEED', f'file_id={file_id} chunks={len(hashes)} OK')
            return file_id

    def _fetch(self, source, metadata, index):
        check(isinstance(source, dict) and identifier(source.get('peer_id'))
              and isinstance(source.get('host'), str)
              and integer(source.get('port'), 1, 65535), 'Invalid source')
        message = dict(version=1, request_id=os.urandom(16).hex(), op='GET_CHUNK',
                       peer_id=self.peer_id, swarm_key=self.swarm_key,
                       file_id=metadata['file_id'], chunk_index=index)
        address = (source['host'], source['port'])
        response, payload = self._exchange(address, message, metadata['chunk_size'])
        check(integer(response.get('version'), 1, 1) and response.get('ok') is True
              and response.get('request_id') == message['request_id']
              and response.get('file_id') == metadata['file_id']
              and integer(response.get('chunk_index'), index, index), 'Invalid chunk response')
        if not self._valid_chunk(metadata, index, payload):
            self.log('HASH_REJECT', f'chunk={index} source={source["peer_id"]}')
            raise PeerError('Chunk hash mismatch')
        return payload

    def _download_chunk(self, metadata, index, cancelled):
        failures = {}
        for attempt in range(self.retries):
            if self.stopping.is_set() or cancelled.is_set():
                raise PeerError('Download cancelled')
            try:
                data = self._rpc('GET_PEERS', file_id=metadata['file_id'], chunk_index=index)
                sources = data.get('peers')
                check(isinstance(sources, list) and len(sources) <= 128, 'Invalid sources')
                if sources:
                    offset = index % len(sources)
                    sources = sources[offset:] + sources[:offset]
                for source in sources:
                    if self.stopping.is_set() or cancelled.is_set():
                        raise PeerError('Download cancelled')
                    key = str(source.get('peer_id')) if isinstance(source, dict) else 'invalid'
                    if failures.get(key, 0) >= 2:
                        continue
                    try:
                        self.log('DOWNLOAD_START', f'chunk={index} source={key}')
                        payload = self._fetch(source, metadata, index)
                    except NETWORK_ERRORS as exc:
                        failures[key] = failures.get(key, 0) + 1
                        self.log('SOURCE_FAILED', f'chunk={index} source={key} {type(exc).__name__}')
                        continue
                    self._store(metadata, index, payload)
                    self.log('CHUNK_OK', f'chunk={index} source={key}')
                    try:
                        self._sync()
                    except NETWORK_ERRORS as exc:
                        self.log('ANNOUNCE_RETRY', type(exc).__name__)
                    return
            except NETWORK_ERRORS as exc:
                self.log('DISCOVERY_RETRY', f'chunk={index} {type(exc).__name__}')
            if self.stopping.wait(min(0.2 * (attempt + 1), 1.0)):
                break
        raise PeerError(f'Chunk {index}: sources exhausted')

    def download(self, file_id):
        check(digest(file_id), 'Invalid file ID')
        with self.operation_lock:
            metadata = validate_metadata(self._rpc('GET_METADATA', file_id=file_id).get('metadata'))
            check(metadata['file_id'] == file_id, 'Metadata ID mismatch')
            self._prepare(metadata)
            self._sync()
            with self.lock:
                pending = [i for i in range(metadata['chunk_count'])
                           if i not in self.files[file_id]['chunks']]
            work_lock = threading.Lock()
            cancelled = threading.Event()
            errors = []

            def work():
                while not self.stopping.is_set() and not cancelled.is_set():
                    with work_lock:
                        if not pending:
                            return
                        index = pending.pop(0)
                    try:
                        self._download_chunk(metadata, index, cancelled)
                    except Exception as exc:
                        with work_lock:
                            errors.append(exc)
                        cancelled.set()

            threads = []
            try:
                for _ in range(min(self.workers, len(pending))):
                    thread = threading.Thread(target=work)
                    thread.start()
                    threads.append(thread)
                for thread in threads:
                    while thread.is_alive():
                        thread.join(0.1)
            except KeyboardInterrupt:
                self._cancel_io()
                raise
            finally:
                cancelled.set()
                for thread in threads:
                    thread.join()
            check(not self.stopping.is_set(), 'Download stopped')
            if errors:
                raise PeerError(str(errors[0]))
            return self._merge(metadata)

    def _merge(self, metadata):
        file_id = metadata['file_id']
        temporary = self._path(file_id, 'assembled.part')
        destination = self._path(file_id, 'download.bin')
        whole_hash = hashlib.sha256()
        size = 0
        try:
            with open(temporary, 'wb') as output:
                for index in range(metadata['chunk_count']):
                    check(not self.stopping.is_set(), 'Merge stopped')
                    with open(self._path(file_id, f'{index}.chunk'), 'rb') as stream:
                        payload = stream.read(metadata['chunk_size'] + 1)
                    check(self._valid_chunk(metadata, index, payload), 'Local chunk corrupted before merge')
                    output.write(payload)
                    whole_hash.update(payload)
                    size += len(payload)
            check(size == metadata['file_size'] and whole_hash.hexdigest() == metadata['file_hash'],
                  'Whole-file hash mismatch')
            os.replace(temporary, destination)
        finally:
            if os.path.isfile(temporary):
                os.remove(temporary)
        self.log('FILE_OK', f'file_id={file_id} path={destination}')
        return destination

    def _cancel_io(self):
        self.stopping.set()
        with self.lifecycle:
            sockets = list(self.sockets)
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            finally:
                sock.close()

    def stop(self):
        self._cancel_io()
        if self.listener:
            self.listener.close()
        for thread in self.background:
            thread.join()
        with self.lifecycle:
            workers = list(self.upload_threads)
        for thread in workers:
            thread.join()
        if self.storage_lock is not None:
            try:
                if self.port:
                    self._rpc('LEAVE')
            except NETWORK_ERRORS:
                pass
            os.close(self.storage_lock)
            self.storage_lock = None
            os.remove(os.path.join(self.storage, '.active'))
        self.log('STOP', 'completed')


def main():
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    peer = None
    try:
        peer_id = os.environ.get('DS02_PEER_ID', '')
        token = os.environ.get('DS02_PEER_TOKEN')
        if token is None:
            keys = json.loads(os.environ.get('DS02_PEER_KEYS', '{}'))
            check(isinstance(keys, dict), 'Invalid peer keys')
            token = keys.get(peer_id, '')
        peer = Peer(peer_id, token,
                    os.environ.get('DS02_SWARM_KEY', ''),
                    int(os.environ.get('DS02_PEER_PORT', '9101')),
                    os.environ.get('DS02_STORAGE', os.path.join('data', peer_id)),
                    tracker_host=os.environ.get('DS02_TRACKER_HOST', '127.0.0.1'),
                    tracker_port=int(os.environ.get('DS02_TRACKER_PORT', '9000')),
                    host=os.environ.get('DS02_PEER_HOST', '127.0.0.1'),
                    workers=int(os.environ.get('DS02_DOWNLOAD_WORKERS', '4')),
                    timeout=float(os.environ.get('DS02_IO_TIMEOUT', '3')),
                    retries=int(os.environ.get('DS02_CHUNK_RETRIES', '5')),
                    upload_delay=float(os.environ.get('DS02_UPLOAD_DELAY', '0')),
                    corrupt_index=int(os.environ.get('DS02_CORRUPT_INDEX', '-1')))
        peer.start()
        action = os.environ.get('DS02_ACTION', 'serve')
        check(action in ('seed', 'download', 'serve'), 'Invalid action')
        if action == 'seed':
            peer.seed(os.environ.get('DS02_FILE', ''), int(os.environ.get('DS02_CHUNK_SIZE', '65536')))
        elif action == 'download':
            peer.download(os.environ.get('DS02_FILE_ID', ''))
        while not peer.stopping.wait(0.5):
            pass
    except KeyboardInterrupt:
        if peer:
            peer.log('INTERRUPT', 'Ctrl+C')
    except (OSError, ValueError, RuntimeError) as exc:
        LOG.error('[PEER] [FAILED] [%s]', str(exc))
        raise SystemExit(1)
    finally:
        if peer:
            peer.stop()


if __name__ == '__main__':
    main()
