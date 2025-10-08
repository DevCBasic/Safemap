#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
classify_data_for_danger.py
Đọc file JSON gốc (mặc định ketqua_valid.json), lọc valid==True và gom nhóm theo:
    linh_vuc -> danh_sach_su_kien (bao gồm Dia_diem, Muc_do_nguy_hiem, url)
Sau đó ghi file ketqua_completed.json với định dạng mới.
"""

import json
import argparse
from collections import OrderedDict
import re

# -------------------------------
# HÀM TIỆN ÍCH
# -------------------------------
def ensure_list(x):
    """Hàm này đảm bảo giá trị trả về luôn là một list."""
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        parts = [p.strip() for p in re.split(r"[;,/|]", x) if p.strip()]
        return parts if parts else [x.strip()]
    return [str(x)]

def get_all_locations(item):
    """Trích xuất và kết hợp tất cả các địa điểm (chính và phụ) thành một danh sách."""
    locations = set()
    main_loc = item.get("location", {}).get("text")
    if main_loc:
        locations.add(main_loc.strip())
    
    alt_locs = item.get("alt_locations")
    if alt_locs:
        for loc in ensure_list(alt_locs):
            locations.add(loc.strip())
            
    if not locations:
        return ["Không rõ"]
    return sorted(list(locations))

def get_linh_vuc(item):
    """Trích xuất danh sách lĩnh vực từ các key có thể có."""
    lv = item.get("Linh_vuc") or item.get("linh_vuc") or item.get("linhVuc")
    lv_list = ensure_list(lv)
    return lv_list if lv_list else ["Không rõ"]

def get_muc_do(item):
    """Trích xuất mức độ khẩn cấp/nguy hiểm."""
    m = item.get("muc_do_khan_cap")
    return ensure_list(m) if m else ["Không xác định"]

# -------------------------------
# GOM NHÓM DỮ LIỆU THEO LĨNH VỰC
# -------------------------------
def group_data_by_linh_vuc(records):
    """
    Gom nhóm các bản ghi dựa trên 'linh_vuc'.
    Mỗi sự kiện được "tách" ra theo từng địa điểm.
    """
    groups = OrderedDict()
    for item in records:
        if not item.get("valid", False):
            continue
        
        linh_vuc_list = get_linh_vuc(item)
        locations_list = get_all_locations(item)
        muc_do = get_muc_do(item)
        urls = item.get("url", [])

        for lv in linh_vuc_list:
            if lv not in groups:
                groups[lv] = {
                    "linh_vuc": lv,
                    "danh_sach_su_kien": []
                }
            
            for loc in locations_list:
                su_kien = {
                    "Dia_diem": loc,
                    "Muc_do_nguy_hiem": muc_do,
                    "url": urls
                }
                groups[lv]["danh_sach_su_kien"].append(su_kien)
                
    return list(groups.values())

# -------------------------------
# GHI FILE FORMATTED THEO CẤU TRÚC MỚI
# -------------------------------
def write_formatted_json(data, filepath):
    """Ghi dữ liệu ra file JSON với định dạng và khoảng trắng cụ thể."""
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i, group in enumerate(data):
            f.write("    {\n")
            f.write(f'        "linh_vuc": "{group["linh_vuc"]}",\n')
            f.write('        "danh_sach_su_kien": [\n')
            
            event_list = group["danh_sach_su_kien"]
            for k, detail in enumerate(event_list):
                f.write("            {\n")
                f.write(f'                "Dia_diem": "{detail["Dia_diem"]}",\n')
                f.write('                "Muc_do_nguy_hiem": [\n')
                for m, md in enumerate(detail["Muc_do_nguy_hiem"]):
                    f.write(f'                    "{md}"{"," if m < len(detail["Muc_do_nguy_hiem"]) - 1 else ""}\n')
                f.write("                ],\n")
                f.write('                "url": [\n')
                urls = detail.get("url", [])
                for n, u in enumerate(urls):
                    f.write(f'                    "{u}"{"," if n < len(urls) - 1 else ""}\n')
                f.write("                ]\n")
                f.write("            }" + ("," if k < len(event_list) - 1 else "") + "\n")
            
            f.write("        ]\n")
            f.write("    }" + ("," if i < len(data) - 1 else "") + "\n")
        f.write("]\n")

# -------------------------------
# MAIN
# -------------------------------
def main():
    """Hàm chính để thực thi script."""
    parser = argparse.ArgumentParser(description="Group and format JSON data by domain (linh_vuc).")
    parser.add_argument("--input", "-i", default="ketqua_valid.json", help="Input JSON file")
    parser.add_argument("--output", "-o", default="ketqua_completed_lv.json", help="Output JSON file")
    args = parser.parse_args()
    
    try:
        with open(args.input, "r", encoding="utf-8") as f:
            records = json.load(f)
        
        grouped_data = group_data_by_linh_vuc(records)
        write_formatted_json(grouped_data, args.output)
        
        print(f"✅ Đã xử lý và ghi dữ liệu thành công vào file: {args.output}")

    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file đầu vào '{args.input}'")
    except json.JSONDecodeError:
        print(f"Lỗi: File '{args.input}' không phải là file JSON hợp lệ.")
    except Exception as e:
        print(f"Đã có lỗi xảy ra: {e}")

if __name__ == "__main__":
    main()