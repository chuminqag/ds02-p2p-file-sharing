import hashlib
import json
import socket
import struct
import threading
import time

from src.protocol import MAX_HEADER, ProtocolError, recv_message, request, send_message
from src.tracker import Tracker


KEYS = {f'peer{i}': f'test-only-secret-for-peer-{i}' for i in range(1, 4)}
FILE_HASH = hashlib.sha256(b'ab').hexdigest()
METADATA = {'file_id': FILE_HASH, 'file_name': 'demo.bin', 'file_size': 2,
            'chunk_size': 1, 'chunk_count': 2,
            'chunk_hashes': [hashlib.sha256(c).hexdigest() for c in (b'a', b'b')],
            'file_hash': FILE_HASH, 'tracker': {'host': '127.0.0.1', 'port': 9000}}


def message(op, peer='peer1', **fields):
    return dict(version=1, request_id='test-request', op=op, peer_id=peer,
                token=KEYS[peer], session_id='session-1', **fields)


def run():
    tracker = Tracker(port=0, peer_keys=KEYS, lease_seconds=3, io_timeout=0.3)
    server = threading.Thread(target=tracker.serve_forever)
    server.start()
    assert tracker.ready.wait(3), 'Tracker did not start'
    passed = []

    def call(op, peer='peer1', **fields):
        return request('127.0.0.1', tracker.port, message(op, peer, **fields))

    def check(name, condition):
        assert condition, name
        passed.append(name)
        print('PASS', name)

    def raw_frame(raw):
        with socket.create_connection(('127.0.0.1', tracker.port), timeout=2) as sock:
            sock.sendall(raw)
            return recv_message(sock, max_payload=0)[0]

    try:
        check('register three peers', all(call('REGISTER_PEER', peer, port=9100 + i)['ok']
                                         for i, peer in enumerate(KEYS)))
        check('register retry is idempotent', call('REGISTER_PEER', port=9100)['ok'])
        check('register immutable metadata', call('REGISTER_FILE', metadata=METADATA)['ok'])
        check('metadata round trip', call('GET_METADATA', file_id=FILE_HASH)['data']['metadata'] == METADATA)
        check('metadata retry', call('REGISTER_FILE', metadata=METADATA)['ok'])
        check('announce partial file', call('ANNOUNCE', file_id=FILE_HASH, revision=1, chunks=[0])['ok'])
        check('discover partial source', len(call('GET_PEERS', 'peer2', file_id=FILE_HASH,
                                                chunk_index=0)['data']['peers']) == 1)
        check('missing chunk has no source', call('GET_PEERS', 'peer2', file_id=FILE_HASH,
                                                 chunk_index=1)['data']['peers'] == [])
        check('heartbeat', call('HEARTBEAT')['ok'])
        check('stale revision rejected', call('ANNOUNCE', file_id=FILE_HASH, revision=0,
                                             chunks=[])['error']['code'] == 'STALE_REVISION')
        check('conflicting retry rejected', call('ANNOUNCE', file_id=FILE_HASH, revision=1,
                                                 chunks=[1])['error']['code'] == 'REVISION_CONFLICT')
        check('invalid chunk rejected', not call('ANNOUNCE', file_id=FILE_HASH,
                                                 revision=2, chunks=[2])['ok'])
        changed = dict(METADATA, chunk_size=2, chunk_count=1, chunk_hashes=[FILE_HASH])
        check('conflicting metadata rejected', call('REGISTER_FILE', metadata=changed)['error']['code']
              == 'METADATA_CONFLICT')
        bad = message('HEARTBEAT')
        bad['token'] = KEYS['peer2']
        check('peer impersonation rejected', request('127.0.0.1', tracker.port, bad)['error']['code']
              == 'UNAUTHORIZED')
        bad['token'] = '\ud800' * 16
        response = request('127.0.0.1', tracker.port, bad)
        check('invalid Unicode token rejected', response['error']['code'] == 'UNAUTHORIZED')
        bad = message('HEARTBEAT')
        bad['session_id'] = 'old-session'
        check('old session rejected', not request('127.0.0.1', tracker.port, bad)['ok'])
        check('oversize prefix rejected', not raw_frame(struct.pack('!I', MAX_HEADER + 1))['ok'])
        check('malformed JSON rejected', not raw_frame(struct.pack('!I', 1) + b'{')['ok'])
        duplicate = b'{"payload_size":0,"payload_size":0}'
        check('duplicate JSON key rejected', not raw_frame(struct.pack('!I', len(duplicate)) + duplicate)['ok'])
        payload_header = json.dumps(dict(message('HEARTBEAT'), payload_size=1)).encode()
        check('tracker refuses file bytes', not raw_frame(struct.pack('!I', len(payload_header))
                                                         + payload_header)['ok'])
        raw = json.dumps(dict(message('HEARTBEAT'), payload_size=0)).encode()
        with socket.create_connection(('127.0.0.1', tracker.port), timeout=2) as sock:
            frame = struct.pack('!I', len(raw)) + raw
            for byte in frame:
                sock.sendall(bytes([byte]))
            check('fragmented TCP frame', recv_message(sock, max_payload=0)[0]['ok'])
        with socket.create_connection(('127.0.0.1', tracker.port), timeout=2) as sock:
            sock.sendall(b'\x00')
            check('incomplete frame times out', recv_message(sock, max_payload=0)[0]['error']['code']
                  == 'BAD_FRAME')
        with socket.create_connection(('127.0.0.1', tracker.port), timeout=2) as sock:
            sock.sendall(b'\x00\x00')
        check('survives abrupt client disconnect', call('HEARTBEAT')['ok'])

        results = []
        result_lock = threading.Lock()

        def announce(revision):
            try:
                result = call('ANNOUNCE', file_id=FILE_HASH, revision=revision,
                              chunks=[0, 1] if revision == 21 else [0])
            except Exception as exc:
                result = exc
            with result_lock:
                results.append(result)

        workers = [threading.Thread(target=announce, args=(i,)) for i in range(2, 22)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(5)
        check('concurrent announcements preserve newest state', len(results) == 20
              and all(isinstance(r, dict) for r in results)
              and len(call('GET_PEERS', 'peer2', file_id=FILE_HASH, chunk_index=1)['data']['peers']) == 1)
        results.clear()

        def concurrent_register():
            try:
                result = call('REGISTER_FILE', metadata=METADATA)
            except Exception as exc:
                result = exc
            with result_lock:
                results.append(result)

        workers = [threading.Thread(target=concurrent_register) for _ in range(12)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(5)
        check('concurrent metadata registration', len(results) == 12
              and all(isinstance(r, dict) and r['ok'] for r in results))
        start = time.monotonic()
        for _ in range(100):
            assert call('GET_METADATA', file_id=FILE_HASH)['ok']
        elapsed = time.monotonic() - start
        print(f'PERF 100 sequential metadata requests: {elapsed:.3f}s, {100 / elapsed:.1f} req/s')
        check('leave removes source', call('LEAVE')['ok']
              and call('GET_PEERS', 'peer2', file_id=FILE_HASH, chunk_index=0)['data']['peers'] == [])
        check('rejoin after leave', call('REGISTER_PEER', port=9100)['ok'])
        assert call('ANNOUNCE', file_id=FILE_HASH, revision=0, chunks=[0])['ok']
        deadline = time.monotonic() + 3.3
        while time.monotonic() < deadline:
            assert call('HEARTBEAT', 'peer2')['ok']
            time.sleep(0.15)
        check('lease expiry removes dead source', call('GET_PEERS', 'peer2', file_id=FILE_HASH,
                                                     chunk_index=0)['data']['peers'] == [])
        check('expired peer must register again', call('HEARTBEAT')['error']['code'] == 'NOT_REGISTERED')
        check('recover after lease expiry', call('REGISTER_PEER', port=9100)['ok'])
        # Exercise two coalesced binary frames independently of tracker RPCs.
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            listener.listen(1)
            with socket.create_connection(listener.getsockname()) as sender:
                receiver, _ = listener.accept()
                with receiver:
                    send_message(sender, {'part': 1}, b'abc')
                    send_message(sender, {'part': 2}, b'def')
                    first = recv_message(receiver)
                    second = recv_message(receiver)
                    check('coalesced frames and binary payload', first[1] == b'abc'
                          and second[1] == b'def' and second[0]['part'] == 2)
        with socket.create_connection(('127.0.0.1', tracker.port), timeout=2) as sock:
            sock.sendall(b'\x00')
            time.sleep(0.05)
            tracker.stop()
            server.join(3)
            check('shutdown releases blocked workers', not server.is_alive() and not tracker.workers)
    finally:
        tracker.stop()
        server.join(3)
    print(f'ALL {len(passed)} CHECKS PASSED')


if __name__ == '__main__':
    run()
