#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from pathlib import Path
import argparse

# ====== ĐƯỜNG DẪN MẶC ĐỊNH ======
# Cấu trúc:
# SAFEMAP/
# ├── Data/
# └── Xu_li_data/
#     └── ket_qua.py (file này)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "Data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INP_DEF = DATA_DIR / "ket_qua.jsonl"          # input
OUT_DEF = DATA_DIR / "ket_qua.valid.json"     # output

ALLOWED_LINH_VUC = {
    "Thiên tai & Môi trường",
    "Giao thông & Hạ tầng",
    "Cháy nổ & Sự cố kỹ thuật",
    "An ninh Trật tự Tội phạm",
    "Cộng đồng & Dịch vụ",
}
ALLOWED_MUC_DO = {"Cảnh báo nguy hiểm", "Cảnh báo trung bình", "Nhắc nhở", "Tích cực"}
ALLOWED_LOC_TYPE = {"ADMIN", "ROAD", "LANDMARK", "COORDS"}
ALLOWED_DISCARD = {"NO_LOCATION", "OUT_OF_SCOPE", "BOTH", "MODEL_MISSED"}

def iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                yield ln, obj
            except Exception as e:
                print(f"[WARN] Dòng {ln} không phải JSON hợp lệ: {e}")

def _is_str_list(x):
    return isinstance(x, list) and all(isinstance(i, str) for i in x)

def validate(obj: dict):
    """
    Kiểm tra theo mẫu trả về mới:
    - Bắt buộc: index:int, valid:bool
    - valid=true:
        * linh_vuc: list[str] ⊆ ALLOWED_LINH_VUC, không rỗng
        * muc_do_khan_cap ∈ ALLOWED_MUC_DO
        * location.text bắt buộc; location.type nếu có phải hợp lệ
        * location.coords (nếu có): lat/lon là số
        * alt_locations: list[str] (có thể rỗng)
        * url: list[str] KHÔNG rỗng (bắt buộc trích xuất)
        * Ngay_thang_nam: list[str] KHÔNG rỗng (bắt buộc trích xuất)
        * confidence: nếu có ∈ [0,1]
        * rationale: khuyến nghị có (nếu thiếu sẽ cảnh báo mềm)
    - valid=false:
        * discard_reason: list[str] KHÔNG rỗng; mỗi phần tử ∈ ALLOWED_DISCARD
        * confidence: nếu có ∈ [0,1]
    """
    errs = []
    warns = []

    # index
    if "index" not in obj or not isinstance(obj["index"], int):
        errs.append("index phải là số nguyên")

    # valid
    valid = obj.get("valid")
    if not isinstance(valid, bool):
        errs.append("valid phải là boolean")

    # confidence
    conf = obj.get("confidence", None)
    if conf is not None and (not isinstance(conf, (int, float)) or not (0.0 <= conf <= 1.0)):
        errs.append("confidence phải là số trong [0,1] nếu có")

    if valid is True:
        # linh_vuc
        lv = obj.get("linh_vuc", [])
        if not _is_str_list(lv) or not lv:
            errs.append("linh_vuc phải là list chuỗi, không rỗng khi valid=true")
        else:
            for x in lv:
                if x not in ALLOWED_LINH_VUC:
                    errs.append(f"linh_vuc '{x}' không thuộc danh sách cho phép")

        # muc_do_khan_cap
        md = obj.get("muc_do_khan_cap")
        if md not in ALLOWED_MUC_DO:
            errs.append("muc_do_khan_cap không hợp lệ")

        # location
        loc = obj.get("location", {})
        if not isinstance(loc, dict) or not loc.get("text"):
            errs.append("location.text bắt buộc khi valid=true")
        else:
            loc_type = loc.get("type")
            if loc_type is not None and loc_type not in ALLOWED_LOC_TYPE:
                errs.append("location.type không hợp lệ (ADMIN|ROAD|LANDMARK|COORDS)")

            coords = loc.get("coords")
            if coords is not None:
                if not isinstance(coords, dict):
                    errs.append("location.coords (nếu có) phải là object")
                else:
                    lat = coords.get("lat")
                    lon = coords.get("lon")
                    if lat is None or lon is None:
                        errs.append("location.coords (nếu có) phải có đủ lat và lon")
                    else:
                        if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
                            errs.append("location.coords.lat/lon phải là số")

        # alt_locations
        alt = obj.get("alt_locations", [])
        if not isinstance(alt, list) or not all(isinstance(x, str) for x in alt):
            errs.append("alt_locations phải là list chuỗi")

        # url (BẮT BUỘC khi valid=true)
        urls = obj.get("url", [])
        if not _is_str_list(urls) or not urls:
            errs.append("url phải là list chuỗi KHÔNG rỗng khi valid=true")

        # Ngay_thang_nam (BẮT BUỘC khi valid=true)
        dates = obj.get("Ngay_thang_nam", [])
        if not _is_str_list(dates) or not dates:
            errs.append("Ngay_thang_nam phải là list chuỗi KHÔNG rỗng khi valid=true")

        # rationale (khuyến nghị có)
        if not isinstance(obj.get("rationale", ""), str) or not obj.get("rationale", "").strip():
            warns.append("rationale nên có (khuyến nghị)")

    elif valid is False:
        dr = obj.get("discard_reason", [])
        if not _is_str_list(dr) or not dr:
            errs.append("discard_reason phải là list chuỗi, không rỗng khi valid=false")
        else:
            for reason in dr:
                if reason not in ALLOWED_DISCARD:
                    errs.append(f"discard_reason '{reason}' không hợp lệ (NO_LOCATION|OUT_OF_SCOPE|BOTH|MODEL_MISSED)")

    return errs, warns

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in",  dest="inp",  default=str(INP_DEF),  help="Đường dẫn ket_qua.jsonl")
    parser.add_argument("--out", dest="out", default=str(OUT_DEF),   help="Đường dẫn ket_qua.valid.json")
    args = parser.parse_args()

    inp_path  = Path(args.inp)
    out_path  = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not inp_path.exists():
        raise FileNotFoundError(f"Không thấy file input: {inp_path}")

    by_index = {}
    errors = []
    warnings = []

    for ln, obj in iter_jsonl(inp_path):
        idx = obj.get("index")
        # Ưu tiên bản ghi đầu tiên theo index
        if isinstance(idx, int) and idx not in by_index:
            by_index[idx] = obj

        errs, warns = validate(obj)
        if errs:
            errors.append((ln, idx, errs))
        if warns:
            warnings.append((ln, idx, warns))

    # Xuất mảng theo thứ tự index tăng dần
    arr = [by_index[k] for k in sorted(by_index)]
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(arr, f, ensure_ascii=False, indent=2)

    print(f"Đã tạo JSON hợp lệ (hình thức): {out_path} (tổng {len(arr)} bản ghi)")

    if warnings:
        print("\nKhuyến nghị (không chặn):")
        for ln, idx, warns in warnings[:80]:
            idx_s = f"index={idx}" if isinstance(idx, int) else "index=?"
            print(f"- Dòng {ln} ({idx_s}): " + "; ".join(warns))
        if len(warnings) > 80:
            print(f"... và {len(warnings)-80} khuyến nghị nữa")

    if errors:
        print("\nCảnh báo/vi phạm schema:")
        for ln, idx, errs in errors[:80]:
            idx_s = f"index={idx}" if isinstance(idx, int) else "index=?"
            print(f"- Dòng {ln} ({idx_s}): " + "; ".join(errs))
        if len(errors) > 80:
            print(f"... và {len(errors)-80} lỗi nữa")
    else:
        print("Tất cả bản ghi đáp ứng kiểm tra cơ bản.")

if __name__ == "__main__":
    main()
