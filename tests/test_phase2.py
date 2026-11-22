import hashlib
import logging
import os
import socket
import threading
import time

from src.peer import Peer, PeerError
from src.protocol import recv_message, send_message
from src.tracker import Tracker


class Records(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


def run():
    root = os.path.abspath(os.path.join('tmp', 'phase2-' + os.urandom(6).hex()))
    os.makedirs(root)
    keys = {f'peer{i}': f'test-only-secret-peer-{i}' for i in range(1, 5)}
    swarm = 'test-only-swarm-key-123456'
    tracker = Tracker(port=0, peer_keys=keys, lease_seconds=2, io_timeout=0.4)
    tracker_thread = threading.Thread(target=tracker.serve_forever)
    tracker_thread.start()
    assert tracker.ready.wait(3)
    recorder = Records()
    logger = logging.getLogger('peer')
    logger.setLevel(logging.INFO)
    logger.addHandler(recorder)
    peers, passed = [], []

    def make(number, **options):
        peer_id = f'peer{number}'
        peer = Peer(peer_id, keys[peer_id], swarm, 0, os.path.join(root, peer_id),
                    tracker_port=tracker.port, timeout=0.5, retries=8, **options)
        peer.start()
        peers.append(peer)
        return peer

    def check(name, condition):
        assert condition, name
        passed.append(name)
        print('PASS', name)

    def expect_error(action):
        try:
            action()
        except (OSError, ValueError):
            return True
        return False

    try:
        path = os.path.join(root, 'source.bin')
        content = os.urandom(65536 * 20 + 123)
        with open(path, 'wb') as stream:
            stream.write(content)
        first = make(1, upload_delay=0.02, corrupt_index=0)
        second = make(2, upload_delay=0.02)
        third = make(3)
        file_id = first.seed(path)
        check('metadata includes final short chunk', first.files[file_id]['metadata']['chunk_count'] == 21)
        check('second independent seed matches ID', second.seed(path) == file_id)
        output = third.download(file_id)
        with open(output, 'rb') as stream:
            check('parallel download whole-file integrity', stream.read() == content)
        check('bad hash rejected and retried from healthy source',
              any('[peer3] [HASH_REJECT] [chunk=0 source=peer1]' in m for m in recorder.messages)
              and any('[peer3] [CHUNK_OK] [chunk=0 source=peer2]' in m for m in recorder.messages))
        check('download uses two sources',
              any('[peer3] [CHUNK_OK]' in m and 'source=peer1' in m for m in recorder.messages)
              and any('[peer3] [CHUNK_OK]' in m and 'source=peer2' in m for m in recorder.messages))
        with socket.create_connection(('127.0.0.1', first.port), timeout=1) as sock:
            send_message(sock, dict(version=1, request_id='unauthorized', op='GET_CHUNK',
                                    peer_id='peer3', swarm_key='wrong-swarm-key-123',
                                    file_id=file_id, chunk_index=1))
            response, payload = recv_message(sock)
            check('upload requires swarm credential', response['ok'] is False and not payload)
        check('path traversal rejected', expect_error(lambda: third.download('../escape')))
        check('duplicate storage instance rejected', expect_error(lambda: Peer(
            'peer4', keys['peer4'], swarm, 0, third.storage, tracker_port=tracker.port).start()))
        third.stop()
        chunk_path = os.path.join(third.storage, file_id, '5.chunk')
        with open(chunk_path, 'wb') as stream:
            stream.write(b'corrupt local cache')
        resumed = make(3)
        start = len(recorder.messages)
        output = resumed.download(file_id)
        messages = recorder.messages[start:]
        check('resume verifies disk and re-downloads only corrupt chunk',
              sum('[peer3] [CHUNK_OK]' in m for m in messages) == 1
              and any('[peer3] [CHUNK_OK] [chunk=5 ' in m for m in messages))
        with open(output, 'rb') as stream:
            check('resumed file hash', hashlib.sha256(stream.read()).hexdigest() == file_id)

        empty = os.path.join(root, 'empty.bin')
        with open(empty, 'wb'):
            pass
        empty_id = second.seed(empty)
        check('empty file supported', os.path.getsize(resumed.download(empty_id)) == 0)

        new_path = os.path.join(root, 'overlap.bin')
        with open(new_path, 'wb') as stream:
            stream.write(os.urandom(65536 * 24))
        first.corrupt_index = -1
        first.upload_delay = 0.05
        overlap_id = first.seed(new_path)
        second.workers = 1
        results = []

        def transfer(peer):
            try:
                results.append(peer.download(overlap_id))
            except Exception as exc:
                results.append(exc)

        downloader = threading.Thread(target=transfer, args=(second,))
        downloader.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with second.lock:
                count = len(second.files.get(overlap_id, {}).get('chunks', []))
            if count >= 2:
                break
            time.sleep(0.01)
        resumed.workers = 2
        overlap_client = threading.Thread(target=transfer, args=(resumed,))
        overlap_client.start()
        downloader.join(15)
        overlap_client.join(15)
        check('two peers download concurrently', not downloader.is_alive()
              and not overlap_client.is_alive() and len(results) == 2
              and all(isinstance(result, str) for result in results))
        check('partial peer uploads before completion', any(
            '[peer2] [UPLOAD]' in m and 'to=peer3 complete=False OK' in m for m in recorder.messages))

        first.upload_delay = 1.0
        second.upload_delay = 0
        fourth = make(4)
        fourth.timeout = 0.1
        output = fourth.download(file_id)
        check('timeout source replaced', any('[peer4] [SOURCE_FAILED]' in m and 'TimeoutError' in m
                                             for m in recorder.messages))
        with open(output, 'rb') as stream:
            check('timeout recovery hash', hashlib.sha256(stream.read()).hexdigest() == file_id)
        second.stop()
        first.stop()
        resumed.stop()
        # Only peer4 remains; an unknown file cannot be downloaded.
        check('unknown file fails without hanging', expect_error(lambda: fourth.download('f' * 64)))
        metadata = dict(fourth.files[file_id]['metadata'])
        metadata['file_hash'] = '0' * 64
        check('whole-file hash mismatch blocks final publication', expect_error(lambda: fourth._merge(metadata)))
        check('failed merge removes temporary output', not os.path.exists(
            os.path.join(fourth.storage, file_id, 'assembled.part')))
        with socket.socket() as stalled_tracker:
            stalled_tracker.bind(('127.0.0.1', 0))
            stalled_tracker.listen()
            stalled_tracker.settimeout(2)
            fourth.tracker_port = stalled_tracker.getsockname()[1]
            fourth.timeout = 20
            interrupted = threading.Thread(target=transfer, args=(fourth,))
            interrupted.start()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                client, _ = stalled_tracker.accept()
                with client:
                    header, _ = recv_message(client, max_payload=0, timeout=1)
                    if header.get('op') != 'GET_METADATA':
                        continue
                    started = time.monotonic()
                    fourth.stop()
                    interrupted.join(2)
                    check('shutdown cancels a pending tracker request',
                          not interrupted.is_alive() and time.monotonic() - started < 3
                          and isinstance(results[-1], (OSError, PeerError)))
                    break
            else:
                raise AssertionError('Tracker did not receive GET_METADATA')
    finally:
        for peer in reversed(peers):
            peer.stop()
        tracker.stop()
        tracker_thread.join(3)
        logger.removeHandler(recorder)
        with open(os.path.join(root, 'events.log'), 'w', encoding='utf-8') as stream:
            stream.write('\n'.join(recorder.messages))
    check('clean shutdown of server and upload threads', not tracker_thread.is_alive()
          and all(not thread.is_alive() for peer in peers for thread in peer.background)
          and all(not peer.upload_threads for peer in peers))
    print(f'ALL {len(passed)} PHASE 2 CHECKS PASSED')
    print('Evidence:', root)


if __name__ == '__main__':
    run()
