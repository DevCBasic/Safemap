#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
classify_data_for_danger.py
Đọc file JSON gốc (mặc định ketqua_valid.json), lọc valid==True và gom nhóm theo:
    Dia_diem -> Thong_tin_chi_tiet (bao gồm Linh_vuc, Muc_do_nguy_hiem, url)
Sau đó ghi file ketqua_completed.json với định dạng được yêu cầu.
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

def get_dia_diem(item):
    """Trích xuất danh sách địa điểm từ trường 'alt_locations'."""
    alts = item.get("alt_locations")
    alts_list = ensure_list(alts)
    return alts_list if alts_list else ["Không rõ"]

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
# GOM NHÓM DỮ LIỆU
# -------------------------------
def group_data(records):
    """Gom nhóm các bản ghi dựa trên danh sách địa điểm ('Dia_diem')."""
    groups = OrderedDict()
    for item in records:
        if not item.get("valid", False):
            continue
        
        dia_list = get_dia_diem(item)
        group_key = tuple(sorted(dia_list))

        if group_key not in groups:
            groups[group_key] = {
                "Dia_diem": dia_list,
                "Thong_tin_chi_tiet": []
            }
        
        # Đã bỏ trường 'index' khỏi đây
        detail = {
            "Linh_vuc": get_linh_vuc(item),
            "Muc_do_nguy_hiem": get_muc_do(item),
            "url": item.get("url", [])
        }
        groups[group_key]["Thong_tin_chi_tiet"].append(detail)
    return list(groups.values())

# -------------------------------
# GHI FILE FORMATTED
# -------------------------------
def write_formatted_json(data, filepath):
    """Ghi dữ liệu ra file JSON với định dạng và khoảng trắng cụ thể."""
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i, group in enumerate(data):
            f.write("{\n")
            f.write('    "Dia_diem": [\n')
            for j, loc in enumerate(group["Dia_diem"]):
                f.write(f'        "{loc}"{"," if j < len(group["Dia_diem"]) - 1 else ""}\n')
            f.write("    ],\n")
            f.write('    "Thong_tin_chi_tiet": [\n')
            for k, detail in enumerate(group["Thong_tin_chi_tiet"]):
                f.write("        {\n")
                # Đã bỏ dòng ghi trường 'index'
                f.write('            "Linh_vuc": [\n')
                for l, lv in enumerate(detail["Linh_vuc"]):
                    f.write(f'                "{lv}"{"," if l < len(detail["Linh_vuc"]) - 1 else ""}\n')
                f.write("            ],\n")
                f.write('            "Muc_do_nguy_hiem": [\n')
                for m, md in enumerate(detail["Muc_do_nguy_hiem"]):
                    f.write(f'                "{md}"{"," if m < len(detail["Muc_do_nguy_hiem"]) - 1 else ""}\n')
                f.write("            ],\n")
                f.write('            "url": [\n')
                urls = detail.get("url", [])
                for n, u in enumerate(urls):
                    f.write(f'                "{u}"{"," if n < len(urls) - 1 else ""}\n')
                f.write("            ]\n")
                f.write("        }" + ("," if k < len(group["Thong_tin_chi_tiet"]) - 1 else "") + "\n")
            f.write("    ]\n")
            f.write("}" + ("," if i < len(data) - 1 else "") + "\n")
        f.write("]\n")

# -------------------------------
# MAIN
# -------------------------------
def main():
    """Hàm chính để thực thi script."""
    parser = argparse.ArgumentParser(description="Group and format JSON data based on locations.")
    parser.add_argument("--input", "-i", default="ketqua_valid.json", help="Input JSON file")
    parser.add_argument("--output", "-o", default="ketqua_completed_dd.json", help="Output JSON file")
    args = parser.parse_args()
    
    try:
        with open(args.input, "r", encoding="utf-8") as f:
            records = json.load(f)
        
        grouped_data = group_data(records)
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