# Triển khai DS02

## Demo Ctrl+C với Tracker và ba peer

Mở bốn tab PowerShell tại thư mục gốc của cùng bản clone. Đặt `DS02_PYTHON`
theo [README](../README.md) rồi chạy khối này trong mỗi tab.

```powershell
$python = (Resolve-Path -LiteralPath $env:DS02_PYTHON).Path
$env:TEMP = Join-Path $PWD.Path 'tmp'
$env:TMP = $env:TEMP
$demo = Join-Path $env:TEMP 'manual-demo'
$env:DS02_TRACKER_HOST = '127.0.0.1'
$env:DS02_TRACKER_PORT = '9000'
```

Trong tab Tracker, tạo khóa, file thử và mở Tracker. Dùng giá trị của
`$keys.peer1`, `$keys.peer2`, `$keys.peer3` và `$swarm` để cấu hình các tab
tương ứng trước khi chạy lệnh cuối. Giữ các khóa demo trong phiên làm việc.

```powershell
$env:DS02_PEER_KEYS = & $python -B -s -m scripts.generate_keys
$keys = $env:DS02_PEER_KEYS | ConvertFrom-Json
$swarm = [Guid]::NewGuid().ToString('N')
New-Item -ItemType Directory -Force $demo | Out-Null
$source = Join-Path $demo 'source.bin'
$bytes = New-Object byte[] (8 * 1024 * 1024 + 123)
$random = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$random.GetBytes($bytes)
$random.Dispose()
[IO.File]::WriteAllBytes($source, $bytes)
& $python -B -s -m src.tracker
```

Trong mỗi tab seeder, thay `$number` bằng 1 hoặc 2 và điền token riêng cùng khóa
swarm. Seeder 1 cố ý gửi một chunk sai hash. Chờ cả hai tab in `SEED`.

```powershell
$number = 1
$env:DS02_PEER_ID = "peer$number"
$env:DS02_PEER_TOKEN = '<token của peer này>'
$env:DS02_SWARM_KEY = '<cùng khóa swarm ở mọi peer>'
$env:DS02_PEER_PORT = [string](9100 + $number)
$env:DS02_STORAGE = Join-Path $demo "peer$number"
$env:DS02_ACTION = 'seed'
$env:DS02_FILE = Join-Path $demo 'source.bin'
$env:DS02_UPLOAD_DELAY = '0.35'
$env:DS02_CORRUPT_INDEX = if ($number -eq 1) { '0' } else { '-1' }
& $python -B -s -m src.peer
```

Trong tab Peer 3, dùng token của Peer 3 và cùng khóa swarm.

```powershell
$env:DS02_PEER_ID = 'peer3'
$env:DS02_PEER_TOKEN = '<token của peer3>'
$env:DS02_SWARM_KEY = '<cùng khóa swarm ở mọi peer>'
$env:DS02_PEER_PORT = '9103'
$env:DS02_STORAGE = Join-Path $demo 'peer3'
$env:DS02_ACTION = 'download'
$env:DS02_DOWNLOAD_WORKERS = '2'
$source = Join-Path $demo 'source.bin'
$env:DS02_FILE_ID = (Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash.ToLowerInvariant()
& $python -B -s -m src.peer
```

Khi Peer 3 ghi `HASH_REJECT` rồi tải được từ cả hai nguồn, nhấn `Ctrl+C` tại tab
Peer 1 trong lúc Peer 3 còn tải. Kiểm tra `SOURCE_FAILED`, `CHUNK_OK` từ Peer 2
và `FILE_OK`. Dùng `Get-FileHash -Algorithm SHA256` để đối chiếu hai file.
Đích tải là `tmp/manual-demo/peer3/<file_id>/download.bin`. Dùng thư mục demo mới
cho lần chạy lại nếu muốn quan sát quá trình tải từ đầu. Kịch bản upload trước khi
tải xong có trong `scripts/test_multiprocess.ps1`.

## Chạy trên LAN

Đặt `DS02_TRACKER_HOST=0.0.0.0` ở tiến trình Tracker. Trong các tiến trình peer,
đặt `DS02_TRACKER_HOST` thành IPv4 LAN của máy Tracker, và `DS02_PEER_HOST` thành
`0.0.0.0` hoặc IPv4 LAN của máy peer. Các peer trên cùng máy dùng cổng upload
khác nhau. Cho phép inbound TCP cho cổng Tracker và các cổng upload trên mạng
Private. Mỗi peer dùng thư mục dữ liệu riêng.

Tracker giữ trạng thái trong RAM. Nếu Tracker khởi động lại, các peer đang chạy
sẽ đăng ký lại metadata và danh sách chunk qua vòng heartbeat.
