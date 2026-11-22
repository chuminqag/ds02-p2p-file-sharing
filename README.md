# DS02 — Hệ thống chia sẻ file P2P mô phỏng BitTorrent

Đồ án Distributed Systems. Tracker quản lý metadata và nguồn chunk. Các peer
chạy độc lập, tải chunk qua TCP và chia sẻ ngay các chunk đã kiểm tra SHA-256.

## Kiến trúc

```text
Peer 1 (seeder) ── metadata / heartbeat / chunk inventory ──┐
Peer 2           ── discovery / announce ──────────────────┼─ Tracker :9000
Peer 3           ── discovery / announce ──────────────────┘
Peer 1 <────────────── chunk bytes ──────────────> Peer 2 <──> Peer 3
```

Tracker lưu metadata và danh sách chunk từng peer trong RAM; không nhận nội dung file.
Mỗi request dùng một TCP connection, một response rồi đóng. Tối đa 32 worker.
Lock bảo vệ trạng thái, không giữ lock trong lúc gửi/nhận socket. Heartbeat giữ lease;
peer hết lease bị loại khỏi kết quả tìm nguồn. Metadata vẫn tồn tại tới khi Tracker dừng.

## Cấu trúc

```text
src/protocol.py          Đóng gói JSON + binary, giới hạn và deadline
src/tracker.py               Đăng ký, metadata, discovery, heartbeat và trạng thái
src/peer.py                  Chia file, upload, download, retry và ghép file
tests/test_phase1.py         Kiểm thử Tracker và framing TCP
tests/test_phase2.py         Kiểm thử peer, toàn vẹn dữ liệu và chịu lỗi
scripts/test_multiprocess.ps1 Demo tự động với Tracker và các peer độc lập
deploy/README.md             Cách chạy thủ công trên localhost/LAN
docs/protocol.md             Giao thức Tracker và trao đổi chunk
docs/phase1-results.md   Kết quả kiểm thử và giới hạn
docs/roadmap.md          Phân công và các phần nộp bài còn lại
.env.example            Cấu hình mẫu, không chứa secret thật
```

## Môi trường và cài đặt

Python 3.10+; mã dự án chỉ dùng `socket`, `threading`, `hashlib`, `json`, `os`,
`struct`, `time`, `logging`. Không có pip dependency, không cần virtualenv.
Clone repository vào ổ D và mở PowerShell tại thư mục gốc của bản clone.
Đặt `DS02_PYTHON` tới `python.exe` phiên bản 3.10+ trên ổ D. Đường dẫn dưới đây
dùng Python 3.12 của máy phát triển. Mỗi máy có thể dùng một đường dẫn riêng.

```powershell
$env:DS02_PYTHON = 'D:\Phân tán\.runtime\python\python.exe'
$python = (Resolve-Path -LiteralPath $env:DS02_PYTHON).Path
$env:TEMP = Join-Path $PWD.Path 'tmp'
$env:TMP = $env:TEMP
New-Item -ItemType Directory -Force $env:TEMP | Out-Null
```

## Chạy Phase 1 trên PowerShell

```powershell
& $python -B -s -m tests.test_phase1
```

Kết quả mong đợi là `ALL 32 CHECKS PASSED`. Test tự mở Tracker trên port ngẫu nhiên,
dừng khi hoàn tất; không cần chạy Tracker riêng trước test.

Tạo khóa riêng cho ba peer trong phiên PowerShell hiện tại rồi chạy Tracker:

```powershell
$env:DS02_PEER_KEYS = & $python -B -s -m scripts.generate_keys
$env:DS02_TRACKER_HOST = '127.0.0.1'
$env:DS02_TRACKER_PORT = '9000'
& $python -B -s -m src.tracker
```

Log mong đợi: `[YYYY-MM-DD HH:MM:SS] [TRACKER] [LISTEN] [127.0.0.1:9000]`.
Nhấn `Ctrl+C` để dừng. Không lưu khóa vào mã nguồn hoặc commit lên Git.
Tracker cần bảng khóa đăng ký. Mỗi tiến trình peer có thể nhận riêng khóa của mình
qua `DS02_PEER_TOKEN`.
`.env.example` là tài liệu cấu hình, không được tự động nạp bởi Python.

## Chạy Phase 2 trên PowerShell

```powershell
& $python -B -s -m tests.test_phase2
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\test_multiprocess.ps1
```

Bộ test Python dùng socket thật và tạo dữ liệu riêng trong `tmp/phase2-*`.
Script PowerShell tự khởi động một Tracker và ba peer trên các port loopback,
rồi chạy hai kịch bản sau.

1. Peer 3 tải song song từ hai nguồn, từ chối chunk sai SHA-256 và tải lại. Script
   tắt Peer 1 khi đang truyền, đối chiếu chunk gặp lỗi với lần tải thành công từ
   Peer 2 và kiểm tra SHA-256 của file ghép.
2. Peer 2 và Peer 3 tải đồng thời. Peer 2 upload chunk đã xác minh trước khi tải
   xong file của mình. Cả hai file ghép được đối chiếu SHA-256 với file gốc.

Script in đường dẫn `Evidence:` chứa log và file tải về, sau đó dọn tiến trình.
`-ExecutionPolicy Bypass` áp dụng cho tiến trình PowerShell chạy demo.
Kết quả đã xác minh trên Windows với Python 3.12 gồm 32/32 kiểm tra Tracker,
20/20 kiểm tra Peer và hai kịch bản demo nhiều tiến trình. Một lần thử Ctrl+C
riêng xác nhận Peer 3 tải lại cùng chunk từ Peer 2 khi Peer 1 đang truyền thì dừng.

- [Chạy từng peer và thử Ctrl+C](deploy/README.md)
- [Giao thức và giới hạn](docs/protocol.md)
- [Kết quả Phase 1](docs/phase1-results.md)
- [Phần việc nộp bài](docs/roadmap.md)
