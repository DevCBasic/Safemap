#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
process_markers.py
Đọc Data/ket_qua.valid.json (hoặc .jsonl), lọc valid=true, geocode location → (lat,lng),
tóm tắt ngắn 'sự kiện' từ noi_dung (qua Gemini, fallback rule-based),
và ghi ra tao_map/data/processed_markers.json theo format yêu cầu.

Cấu trúc dự án giả định:
SAFEMAP/
├── Data/
│   ├── ket_qua.valid.json
│   └── ket_qua.jsonl (tuỳ)
├── Xu_li_data/
│   └── process_markers.py   ← file này
└── tao_map/
    └── data/
        └── processed_markers.json  ← output
"""

from pathlib import Path
import argparse
import json
import time
import re
import requests

# ====== ĐƯỜNG DẪN MẶC ĐỊNH ======
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "Data"
OUT_DIR      = PROJECT_ROOT / "tao_map" / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)

INP_DEF  = DATA_DIR / "ket_qua.valid.json"      # mặc định đọc file JSON mảng
OUT_DEF  = OUT_DIR / "processed_markers.json"   # xuất mảng markers

# ====== GEOCODING (Nominatim) ======
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
CONTACT_EMAIL = "you@example.com"  # ← NÊN đổi thành email của bạn

def geocode_location(text: str, country_bias: str = "VN", sleep_sec: float = 1.0):
    """
    Geocode chuỗi địa điểm sang (lat, lon) dùng Nominatim (keyless).
    Trả về (lat, lon) dạng float, hoặc (None, None) nếu thất bại.
    """
    if not text or not text.strip():
        return (None, None)
    params = {
        "q": text,
        "format": "json",
        "addressdetails": 0,
        "limit": 1,
        "countrycodes": country_bias.lower() if country_bias else None,
        "accept-language": "vi",
        "email": CONTACT_EMAIL,
    }
    headers = {
        "User-Agent": f"SafeMap-Geocoder/1.0 (+{CONTACT_EMAIL})"
    }
    try:
        r = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=15)
        if r.status_code == 200:
            arr = r.json()
            if isinstance(arr, list) and arr:
                lat = float(arr[0]["lat"])
                lon = float(arr[0]["lon"])
                time.sleep(sleep_sec)  # lịch sự với public API
                return (lat, lon)
        # Thất bại → thử không country bias
        params.pop("countrycodes", None)
        r2 = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=15)
        if r2.status_code == 200:
            arr = r2.json()
            if isinstance(arr, list) and arr:
                lat = float(arr[0]["lat"])
                lon = float(arr[0]["lon"])
                time.sleep(sleep_sec)
                return (lat, lon)
    except Exception:
        pass
    return (None, None)

# ====== TÓM TẮT SỰ KIỆN ======
def summarize_event_gemini(text: str, max_words: int = 12):
    """
    Tóm tắt ngắn gọn bằng Google Gemini (google.genai).
    Trả về chuỗi <= ~max_words, fallback nếu lỗi hoặc SDK không có.
    """
    text = (text or "").strip()
    if not text:
        return ""
    try:
        from google import genai
        from google.genai import types
        client = genai.Client()
        prompt = (
            "Tóm tắt siêu ngắn (<= {n} từ) một mô tả sự cố/sự kiện, giữ trọng tâm, tiếng Việt, "
            "không thêm tiền tố: \n\n\"{content}\""
        ).format(n=max_words, content=text[:2000])
        resp = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(thinking_budget=0),
            ),
        )
        out = (resp.text or "").strip()
        # làm gọn lại: bỏ xuống dòng, ràng buộc từ
        out = re.sub(r"\s+", " ", out)
        words = out.split()
        if len(words) > max_words:
            out = " ".join(words[:max_words])
        # bỏ dấu ngoặc kép bao ngoài nếu có
        out = out.strip("“”\"'")
        return out
    except Exception:
        return summarize_event_fallback(text, max_words=max_words)

def summarize_event_fallback(text: str, max_words: int = 12):
    """
    Fallback rule-based: lấy nhãn/điểm nhấn đầu từ title/summary.
    Cắt gọn ~max_words.
    """
    text = re.sub(r"\s+", " ", (text or "")).strip()
    # ưu tiên lấy 1 câu đầu
    sentence = re.split(r"[.!?]\s+", text, maxsplit=1)[0]
    words = sentence.split()
    if len(words) > max_words:
        sentence = " ".join(words[:max_words])
    # dọn rác dấu câu
    sentence = sentence.strip("–-,:;— ")
    return sentence

# ====== MAP MỨC ĐỘ KHẨN CẤP ======
MD_MAP = {
    "Cảnh báo nguy hiểm": "Nguy hiểm",
    "Cảnh báo trung bình": "Trung bình",
    "Nhắc nhở": "Nhắc nhở",
    "Tích cực": "Tích cực",
}

# ====== ĐỌC INPUT (json/jsonl) ======
def load_items(inp: Path):
    """
    Trả về list object. Hỗ trợ:
    - .json  (mảng)
    - .jsonl (mỗi dòng 1 object)
    - hoặc file .json chứa 1 object đơn lẻ (sẽ thành list [obj])
    """
    if not inp.exists():
        raise FileNotFoundError(f"Không thấy file input: {inp}")
    if inp.suffix.lower() == ".jsonl":
        out = []
        with inp.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    out.append(obj)
                except Exception:
                    pass
        return out
    else:
        with inp.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        else:
            return []

# ====== MAIN ======
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in",  dest="inp",  default=str(INP_DEF), help="Đường dẫn input (ket_qua.valid.json hoặc .jsonl)")
    ap.add_argument("--out", dest="out", default=str(OUT_DEF),  help="Đường dẫn output processed_markers.json")
    ap.add_argument("--country", default="VN", help="Ưu tiên geocode trong country code (VD: VN)")
    ap.add_argument("--sleep", type=float, default=1.0, help="Delay giữa các lần gọi Nominatim (giây)")
    ap.add_argument("--max-words", type=int, default=12, help="Số từ tối đa cho tóm tắt sự kiện")
    args = ap.parse_args()

    inp_path = Path(args.inp)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    items = load_items(inp_path)

    markers = []
    for obj in items:
        # chỉ xử lý valid=true
        if not isinstance(obj, dict) or obj.get("valid") is not True:
            continue

        # Lấy địa điểm gốc
        loc_text = (obj.get("location") or {}).get("text") or ""
        lat, lon = geocode_location(loc_text, country_bias=args.country, sleep_sec=args.sleep)
        if lat is None or lon is None:
            # Không có toạ độ thì bỏ qua record (đúng yêu cầu “tạo đọ lấy từ location (gọi API để lấy)”)
            continue

        # Tóm tắt “sự kiện” từ noi_dung (qua Gemini; fallback rule-based)
        noi_dung = obj.get("noi_dung", "")
        su_kien  = summarize_event_gemini(noi_dung, max_words=args.max_words)

        # Map mức độ
        muc_goc = obj.get("muc_do_khan_cap")
        muc_out = MD_MAP.get(muc_goc, muc_goc or "")

        markers.append({
            "lat": float(lat),
            "lng": float(lon),
            "sự kiện": su_kien,
            "mức độ khẩn cấp": muc_out,
            "nguồn": ""  # tạm thời chưa xử lý theo yêu cầu
        })

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(markers, f, ensure_ascii=False, indent=2)

    print(f"✓ Đã tạo {len(markers)} marker → {out_path}")

if __name__ == "__main__":
    main()
