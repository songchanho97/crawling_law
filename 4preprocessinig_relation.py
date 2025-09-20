# -*- coding: utf-8 -*-
"""
입력: 산업안전보건법_dedup.json (루트: list)
동작:
  1) refs가 비어있지 않은 노드만 대상으로, 각 refs의 ref.id로 대상 노드 text를 같은 JSON에서 찾아 붙임
  2) (src_id, src_law_title, src_level, src_number, src_text) 그룹 내부에서
     ref_label 이 중복이면 첫 번째만 남김(기본은 대소문자/공백 무시)
출력: refs_from_json_dedup.xlsx
"""

import json
import pandas as pd
from typing import Any, Dict, List, Optional

# ---------------------------
# 경로/옵션
# ---------------------------

file_name = "./data/산업안전보건법_시행령"

INPUT_JSON  = f"{file_name}_dedup.json"
OUTPUT_XLSX = f"{file_name}_refs_from_json_dedup.xlsx"

# 본문 길이 제한(None이면 전체)
TRUNCATE_SRC_TEXT = None   # 예: 200
TRUNCATE_REF_TEXT = None   # 예: 200

# ref_label 중복 판정 시 대소문자/양쪽 공백 무시
CASE_INSENSITIVE_LABEL = True

# ---------------------------
# 유틸
# ---------------------------
def truncate_text(s: Optional[str], limit: Optional[int]) -> str:
    if s is None:
        return ""
    if limit is None or limit <= 0:
        return str(s)
    s = str(s)
    return s if len(s) <= limit else (s[:limit] + " …")

def refs_is_nonempty(node: Dict[str, Any]) -> bool:
    r = node.get("refs", [])
    return isinstance(r, list) and len(r) > 0

def node_text(node: Dict[str, Any]) -> str:
    return str(node.get("text", "") or "")

def get_ref_label(ref: Dict[str, Any]) -> str:
    return str(ref.get("label", "") or "")

def get_ref_law_title(ref: Dict[str, Any]) -> str:
    # 이전 파이프라인 호환: lab_title 키도 보조로 확인
    return str(ref.get("law_title", ref.get("lab_title", "")) or "")

def label_norm(label: str) -> str:
    return label.strip().lower() if CASE_INSENSITIVE_LABEL else label

# ---------------------------
# 메인
# ---------------------------
def main():
    # 1) JSON 로드
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("입력 JSON의 루트는 list여야 합니다.")

    # 2) id -> node 매핑
    id2node: Dict[str, Dict[str, Any]] = {}
    for n in data:
        if isinstance(n, dict):
            nid = str(n.get("id", "") or "")
            if nid:
                id2node[nid] = n

    # 3) rows 생성 (refs 보유 노드만)
    rows: List[Dict[str, Any]] = []
    for node in data:
        if not isinstance(node, dict) or not refs_is_nonempty(node):
            continue

        src_id         = str(node.get("id", "") or "")
        src_law_title  = str(node.get("law_title", "") or "")
        src_level      = str(node.get("level", "") or "")
        src_number     = str(node.get("number", "") or "")
        src_text_out   = truncate_text(node_text(node), TRUNCATE_SRC_TEXT)

        refs = node.get("refs", [])
        for idx, ref in enumerate(refs, start=1):
            ref_id   = str(ref.get("id", "") or "")
            ref_lbl  = get_ref_label(ref)
            ref_law  = get_ref_law_title(ref)

            target = id2node.get(ref_id)
            if target is not None:
                ref_text_out = truncate_text(node_text(target), TRUNCATE_REF_TEXT)
                ref_found = True
            else:
                ref_text_out = ""
                ref_found = False

            rows.append({
                "src_id":        src_id,
                "src_law_title": src_law_title,
                "src_level":     src_level,
                "src_number":    src_number,
                "src_text":      src_text_out,

                "ref_index":     idx,
                "ref_label":     ref_lbl,
                "ref_law_title": ref_law,
                "ref_id":        ref_id,
                "ref_text":      ref_text_out,
                "ref_found":     ref_found,
            })

    # 4) DataFrame화
    df = pd.DataFrame(rows, columns=[
        "src_id","src_law_title","src_level","src_number","src_text",
        "ref_index","ref_label","ref_law_title","ref_id","ref_text","ref_found"
    ])

    # 5) 같은 src 그룹 내 ref_label 중복 제거
    #    그룹키: src_id, src_law_title, src_level, src_number, src_text
    #    라벨키: ref_label(정규화)
    if not df.empty:
        df["_label_norm"] = df["ref_label"].fillna("").astype(str).apply(label_norm)
        key_cols = ["src_id","src_law_title","src_level","src_number","src_text","_label_norm"]
        before = len(df)
        df = df.drop_duplicates(subset=key_cols, keep="first").copy()
        df.drop(columns=["_label_norm"], inplace=True)
        after = len(df)
        print(f"[INFO] 중복 제거: {before} -> {after} (삭제 {before - after})")

    # 6) 저장
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="refs_lookup_dedup", index=False)

    print(f"[OK] 저장 완료 → {OUTPUT_XLSX}")
    print(f"- 결과 행수: {len(df)}")

if __name__ == "__main__":
    main()
