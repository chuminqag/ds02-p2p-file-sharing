# Giao thức DS02 v1

## TCP framing

Mỗi frame: `uint32 big-endian header_length | JSON UTF-8 header | binary payload`.
Header bắt buộc có `payload_size` là số nguyên không âm. `send_message` tự chèn
trường này. `recv_message` đọc chính xác từng phần bằng vòng lặp, xử lý cả TCP
phân mảnh và nhiều frame liền nhau; không dựa vào một lần `recv` trả đủ dữ liệu.

Header tối đa 1 MiB; payload tối đa 1 MiB. Tracker đặt giới hạn payload bằng **0**
và từ chối ngay header khai báo payload khác 0. Frame nhận/gửi có deadline tổng
5 giây mặc định, không gia hạn vô tận khi client gửi nhỏ giọt. JSON trùng key,
NaN, Infinity, UTF-8 lỗi và kích thước vượt giới hạn bị từ chối.

Tracker xử lý đúng một request và một response trên mỗi kết nối. Mở kết nối mới
cho request kế tiếp. Peer dùng cùng framing để truyền binary payload qua GET_CHUNK.

## Request chung

```json
{
  "version": 1,
  "request_id": "request-001",
  "op": "HEARTBEAT",
  "peer_id": "peer1",
  "token": "<secret riêng của peer1>",
  "session_id": "<ID phiên hiện tại>",
  "payload_size": 0
}
```

Các ID peer/request/session dài 1..64 ký tự ASCII chữ, số, `_`, `-`.
Peer tạo session mới bằng `os.urandom(16).hex()` mỗi lần khởi động và giữ nguyên
trong phiên. Request ID dùng đối chiếu response; không phải khóa cache response.
Peer ID phải được cấp trước qua `DS02_PEER_KEYS`, ánh xạ JSON ID → secret riêng.
Mọi API đều xác thực. Tracker giữ SHA-256 của secret trong đối tượng cấu hình,
không ghi token hay toàn bộ request ra log. Địa chỉ IPv4 nguồn lấy từ socket.

## API

| op | Trường bổ sung | data trả về |
|---|---|---|
| REGISTER_PEER | port | lease_seconds |
| HEARTBEAT | không | lease_seconds |
| REGISTER_FILE | metadata | file_id |
| GET_METADATA | file_id | metadata |
| ANNOUNCE | file_id, revision, chunks | revision |
| GET_PEERS | file_id, chunk_index | chunk_index, peers |
| LEAVE | không | object rỗng |

Ngoài REGISTER_PEER, mọi API yêu cầu peer có phiên còn hạn, đúng session và IP.
REGISTER_PEER lặp lại cùng session/endpoint chỉ gia hạn; session/endpoint khác bị
SESSION_CONFLICT cho tới LEAVE hoặc hết lease. Peer đồng bộ lại phiên và inventory
mỗi giây, hoặc sau một phần ba lease nếu lease ngắn hơn ba giây.
Chỉ REGISTER_PEER và HEARTBEAT gia hạn lease.
LEAVE lặp sau khi đã xóa trả NOT_REGISTERED; client coi đây là đã rời mạng.

REGISTER_FILE chỉ đăng ký metadata, **không tự tuyên bố peer giữ chunk**.
Seeder phải gửi ANNOUNCE sau đăng ký. GET_PEERS chỉ trả nguồn giữ chunk yêu cầu,
không trả chính người hỏi. Danh sách là gợi ý tại thời điểm tra cứu: nguồn vẫn có
thể chết ngay sau đó, peer tải sẽ thử nguồn khác với số lần thử hữu hạn.

ANNOUNCE là snapshot đầy đủ các chunk hiện có, chỉ số bắt đầu từ 0, không trùng.
`revision` là số nguyên 0..2^53−1, tăng riêng cho mỗi file trong một session.
Revision thấp hơn bị từ chối; cùng revision và cùng snapshot là retry hợp lệ;
cùng revision nhưng snapshot khác là xung đột. Snapshot rỗng bỏ mọi chunk của
peer cho file đó. Sau đăng ký phiên mới, gửi lại inventory với revision từ 0.

## Metadata

```json
{
  "file_id": "<SHA-256 toàn file, 64 hex thường>",
  "file_name": "demo.bin",
  "file_size": 131072,
  "chunk_size": 65536,
  "chunk_count": 2,
  "chunk_hashes": ["<SHA-256 chunk 0>", "<SHA-256 chunk 1>"],
  "file_hash": "<giống file_id>",
  "tracker": {"host": "127.0.0.1", "port": 9000}
}
```

Tối đa 256 MiB/file, 4096 chunk/file, 1 MiB/chunk, 256 file và 128 peer được
cấu hình. Chunk mặc định 64 KiB. Số chunk phải bằng ceil(file_size/chunk_size).
File rỗng có 0 chunk, mảng hash rỗng và SHA-256 của bytes rỗng. Tên file không
được chứa đường dẫn, ký tự điều khiển hoặc ký tự đặc biệt Windows. Peer lưu dữ
liệu dưới thư mục File ID và dùng các tên đích cố định theo giao thức.

File ID = whole-file hash; metadata đầu tiên bất biến. Cùng bytes nhưng đổi tên,
chunk size hoặc tracker endpoint sẽ báo METADATA_CONFLICT: sử dụng metadata đã
công bố. Đây là quy ước giản lược của đồ án, không phải BitTorrent infohash.
Tracker kiểm tra cấu trúc hash; chỉ peer có bytes mới kiểm chứng được hash thật.

## Response và xử lý lỗi

```json
{"version":1,"request_id":"request-001","ok":true,"data":{"lease_seconds":30},"payload_size":0}
```

```json
{"version":1,"request_id":"request-001","ok":false,"error":{"code":"NOT_REGISTERED","message":"Register peer first"},"payload_size":0}
```

Frame hỏng có thể có request_id null; kết nối đã hỏng thì không bảo đảm gửi được
response. Mã lỗi: INVALID_REQUEST, UNAUTHORIZED, NOT_REGISTERED, SESSION_CONFLICT,
NOT_FOUND, METADATA_CONFLICT, STALE_REVISION, REVISION_CONFLICT, CAPACITY, BAD_FRAME,
INTERNAL_ERROR. Khi hết 32 slot, Tracker đóng kết nối mới và log BUSY.

Peer retry lỗi mạng với số lần hữu hạn. Khi Tracker khởi động lại, peer đăng ký
lại phiên, metadata và inventory đã có. REGISTER_FILE và ANNOUNCE hỗ trợ lặp
yêu cầu theo quy tắc ở trên.

## GET_CHUNK giữa hai peer

Peer tải mở một kết nối TCP đến địa chỉ upload mà Tracker trả về, gửi header có
`version`, `request_id`, `op: "GET_CHUNK"`, `peer_id`, `swarm_key`, `file_id`
và `chunk_index`. Request có `payload_size` bằng 0. Nguồn chỉ chấp nhận yêu cầu
có khóa swarm hợp lệ và chunk còn sẵn trong danh sách đã xác minh.

Response thành công có `ok: true`, cùng `request_id`, `file_id` và `chunk_index`,
kèm bytes thô. Response lỗi có `ok: false`, `error` và payload rỗng. Peer tải
giới hạn payload theo kích thước chunk trong metadata, kiểm tra độ dài thực tế
và SHA-256 rồi mới ghi file chunk, cập nhật inventory và phục vụ upload.

Mỗi peer phục vụ tối đa 16 upload cùng lúc và mặc định có 4 worker tải. Nếu nguồn
timeout, ngắt kết nối, trả lỗi hoặc gửi chunk sai hash, worker thử nguồn khác và
tra cứu lại trong giới hạn `DS02_CHUNK_RETRIES`. Peer ghép chunk theo chỉ số và
chỉ xuất `download.bin` khi tổng độ dài và SHA-256 khớp metadata.

## Tính nhất quán và giới hạn

Một lock tuần tự hóa thao tác trên state trong Tracker; socket I/O chạy ngoài lock.
Discovery loại lease hết hạn trước khi trả kết quả, vòng accept cũng dọn mỗi ~0.2s.
Tính sống còn của nguồn là eventual qua lease, không phải bảo đảm nguồn đang sống.
Heartbeat không chứng minh port upload thật sự đang lắng nghe.

Tracker là single point of failure và state không bền vững. TCP chưa mã hóa;
secret gửi qua kết nối chỉ phù hợp phạm vi localhost/LAN tin cậy của đồ án.
Chưa có TLS, ACL theo file hoặc xác minh quyền sở hữu bytes; thành viên đã cấp
khóa có quyền công bố/đọc metadata và chỉ được sửa inventory của chính mình.
