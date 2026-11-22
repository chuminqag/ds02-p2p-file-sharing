# Kết quả Phase 1

Ngày kiểm tra 22/11/2026 với Windows và Python 3.12 từ `.runtime/python` trên ổ D.
Chạy `& '.\.runtime\python\python.exe' -B -s -m tests.test_phase1`.
**32/32 kiểm tra đạt**, gồm kiểm tra từ chối token Unicode không hợp lệ. Tiến trình kiểm thử trả mã 0.

| Nhóm | Bằng chứng |
|---|---|
| Chức năng | Đăng ký 3 peer, metadata round trip, announce từng phần, discovery, heartbeat, leave/rejoin |
| Đồng thời | 20 announce tranh chấp giữ revision mới nhất; 12 đăng ký metadata đồng thời |
| Chịu lỗi | Client đứt giữa frame; timeout frame thiếu; hết lease, đăng ký phục hồi; shutdown worker đang chờ |
| Dữ liệu/bảo mật | Sai khóa, token Unicode không hợp lệ, sai session, chỉ số chunk sai, metadata conflict, prefix quá lớn, JSON lỗi/trùng key, payload bị từ chối |
| TCP | Header gửi từng byte; hai frame binary gửi liên tiếp, đọc riêng đúng nội dung |
| Hiệu năng | 100 request GET_METADATA tuần tự: 0.797 giây, khoảng 125.5 request/giây |

Số đo là một lần smoke benchmark loopback, mỗi request mở TCP mới; không phải
cam kết tải cao, chưa đo CPU/RAM, percentile hoặc lưu lượng chunk.
Test dùng Tracker và client qua socket thật trong một process để kiểm tra logic;
chưa thay thế yêu cầu demo Tracker + ít nhất 3 peer ở các process độc lập.
Bộ test xác nhận `stop()` giải phóng worker đang đợi frame. Chạy Tracker độc lập
trong PowerShell rồi nhấn `Ctrl+C` cho ra log `[STOP] [completed]` và
`[INTERRUPT] [Ctrl+C]`.

Chia/ghép file, upload/download đồng thời, chuyển nguồn, từ chối chunk sai hash và
kiểm tra hash toàn file thuộc bộ kiểm thử Phase 2. Tracker Phase 1 giữ metadata
và danh sách chunk trong RAM.
