import redis
import requests
import pytest

# --- Cấu hình ---
API_BASE_URL = "http://localhost:8000"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

def get_valid_playlist_ids_from_redis(count=5):
    """
    Hàm phụ trợ để kết nối tới Redis và lấy ra một danh sách
    các playlist_id hợp lệ để test.
    """
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        r.ping() # Kiểm tra kết nối
    except redis.exceptions.ConnectionError:
        pytest.fail(f"Không thể kết nối tới Redis tại {REDIS_HOST}:{REDIS_PORT}. "
                    "Hãy đảm bảo Redis đang chạy qua `docker compose up`.")

    valid_ids = []
    
    # --- [SỬA LỖI TẠI ĐÂY] ---
    # Thay đổi prefix từ "recommendations:*" thành "playlist:*"
    # để khớp với dữ liệu Spark đã ghi vào.
    iterator = r.scan_iter("playlist:*", count=100) 
    
    for key in iterator:
        # Key có dạng b'playlist:123', decode và tách số ID ra
        # split(":")[1] sẽ lấy phần số phía sau dấu hai chấm
        try:
            playlist_id = key.decode('utf-8').split(":")[1]
            valid_ids.append(playlist_id)
        except IndexError:
            continue # Bỏ qua nếu key không đúng định dạng
            
        if len(valid_ids) >= count:
            break
    
    if not valid_ids:
        pytest.fail("Không tìm thấy bất kỳ key 'playlist:*' nào trong Redis. "
                    "Hãy đảm bảo job pre-computation đã chạy thành công.")
        
    return valid_ids

# --- Các bài test ---

def test_api_root_endpoint():
    """Kiểm tra endpoint gốc có hoạt động không."""
    response = requests.get(API_BASE_URL + "/")
    assert response.status_code == 200
    # Cập nhật message cho khớp với code API mới nhất của bạn (nếu cần)
    # assert response.json() == {"message": "Spotify Lakehouse API is Running!"} 

# Lấy danh sách ID hợp lệ một lần duy nhất để dùng cho các bài test
VALID_IDS_TO_TEST = get_valid_playlist_ids_from_redis(count=5)

@pytest.mark.parametrize("playlist_id", VALID_IDS_TO_TEST)
def test_recommendation_endpoint_with_valid_ids(playlist_id):
    """
    Kiểm tra endpoint gợi ý với các ID chắc chắn có trong Redis.
    """
    print(f"\nTesting with valid playlist_id: {playlist_id}")
    
    response = requests.get(f"{API_BASE_URL}/recommendations/{playlist_id}")
    
    # 1. Kiểm tra mã 200 OK
    assert response.status_code == 200, \
        f"Lỗi! API trả về {response.status_code} cho ID hợp lệ {playlist_id}. " \
        f"Nội dung lỗi: {response.text}"

    # 2. Kiểm tra cấu trúc dữ liệu
    data = response.json()
    assert "playlist_id" in data
    assert "recommended_tracks" in data
    # Chuyển cả 2 về string để so sánh cho an toàn
    assert str(data["playlist_id"]) == str(playlist_id) 
    assert isinstance(data["recommended_tracks"], list)
    # Kiểm tra list không rỗng
    assert len(data["recommended_tracks"]) > 0
    
    print(f"✅ PASSED for playlist_id: {playlist_id}")


def test_recommendation_endpoint_with_invalid_id():
    """Kiểm tra xem API có trả về lỗi 404 cho một ID không tồn tại không."""
    invalid_id = "999999999" 
    print(f"\nTesting with invalid playlist_id: {invalid_id}")
    
    response = requests.get(f"{API_BASE_URL}/recommendations/{invalid_id}")
    
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()
    
    print(f"✅ PASSED for invalid playlist_id: {invalid_id}")

if __name__ == "__main__":
    # Chạy test thủ công nếu file này được chạy trực tiếp
    test_api_root_endpoint()
    for pid in VALID_IDS_TO_TEST:
        test_recommendation_endpoint_with_valid_ids(pid)
    test_recommendation_endpoint_with_invalid_id()    