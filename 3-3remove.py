# -*- coding: utf-8 -*-
"""
엑셀의 '링크데이터_JSON' 값들을 가공 없이 그대로 꺼내
기존 산업안전보건법.json (루트: 리스트) 뒤에 append/extend 하는 스크립트.

- id 매칭 안 함
- 구조/키 변경 안 함 (label 등 추가 X)
- 중복 제거 안 함
"""

import json
import pandas as pd
from typing import Any, List, Dict, Optional

file_name = "./data/산업안전보건법_시행령"

# -----------------------------
# 경로 설정
# -----------------------------
EXCEL_PATH = f"{file_name}_Ref_labeled_with_json.xlsx"  # 입력 엑셀
SHEET_NAME = None  # None이면 첫 시트
LINK_JSON_COLS = ["링크데이터_JSON", "링크데이터_json"]  # 후보 컬럼명
JSON_IN_PATH = f"{file_name}_refs_filled.json"  # 기존 JSON (루트: list)
JSON_OUT_PATH = f"{file_name}_merged.json"  # 병합 결과 저장


# -----------------------------
# 유틸
# -----------------------------
def find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """후보 컬럼명 리스트 중 실제 존재하는 첫 번째 컬럼명 반환 (공백/대소문자 무시)."""
    norm = {c.lower().replace(" ", ""): c for c in df.columns}
    for want in candidates:
        key = want.lower().replace(" ", "")
        if key in norm:
            return norm[key]
    return None


def safe_json_loads(cell: Any) -> Optional[Any]:
    """셀의 문자열을 JSON으로 파싱. 비거나 NaN이면 None."""
    if cell is None:
        return None
    if isinstance(cell, float) and pd.isna(cell):
        return None
    if isinstance(cell, (list, dict)):
        return cell
    text = str(cell).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        # 우리 파이프라인이 만든 JSON이면 일반적으로 여기 안 옴.
        # 그래도 혹시 모를 작은따옴표 등 최소한의 보정 시도
        try:
            text2 = text.replace("'", '"')
            return json.loads(text2)
        except Exception:
            return None


def flatten_link_json(val: Any) -> List[Dict[str, Any]]:
    """배열이면 원소들 그대로, 단일 객체면 1개 리스트로, 그 외/실패 시 빈 리스트."""
    parsed = safe_json_loads(val)
    if parsed is None:
        return []
    if isinstance(parsed, list):
        # 원소가 dict가 아닐 수도 있지만, "그대로" 요구 → 필터링 없이 넣을 수도 있음.
        # 다만 기본적으로 dict만 기대되므로 dict만 취하고 싶다면 아래 주석 해제.
        # return [x for x in parsed if isinstance(x, dict)]
        return parsed
    if isinstance(parsed, dict):
        return [parsed]
    return []


# -----------------------------
# 메인
# -----------------------------
def main():
    # 1) 기존 JSON 로드 (루트가 리스트라고 가정; dict면 리스트로 감쌈)
    with open(JSON_IN_PATH, "r", encoding="utf-8") as f:
        base = json.load(f)
    if isinstance(base, dict):
        base_list = [base]
    elif isinstance(base, list):
        base_list = base
    else:
        raise ValueError("기존 JSON의 루트가 list/dict가 아닙니다.")

    original_len = len(base_list)

    # 2) 엑셀 로드
    xls = pd.ExcelFile(EXCEL_PATH)
    sheet = (
        SHEET_NAME
        if (SHEET_NAME and SHEET_NAME in xls.sheet_names)
        else xls.sheet_names[0]
    )
    df = pd.read_excel(xls, sheet_name=sheet, dtype=str)

    # 3) 링크데이터_JSON 컬럼 탐색
    link_col = find_column(df, LINK_JSON_COLS)
    if not link_col:
        raise ValueError(
            f"엑셀에 '{LINK_JSON_COLS}' 중 어느 컬럼도 없습니다. 실제 컬럼: {list(df.columns)}"
        )

    # 4) 전 행 스캔 → 그대로 수집
    added_items: List[Any] = []
    total_cells = 0
    nonempty_cells = 0

    for _, row in df.iterrows():
        total_cells += 1
        raw = row.get(link_col)
        items = flatten_link_json(raw)
        if items:
            nonempty_cells += 1
            # "그대로" 이어붙이기
            added_items.extend(items)

    # 5) 기존 리스트 뒤에 그대로 이어붙임 (extend)
    base_list.extend(added_items)

    # 6) 저장
    with open(JSON_OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(base_list, f, ensure_ascii=False, indent=2)

    print(f"[OK] 병합 완료 → {JSON_OUT_PATH}")
    print(f"- 기존 JSON 항목수: {original_len}")
    print(f"- 엑셀 스캔 행수: {total_cells} (비어있지 않은 셀: {nonempty_cells})")
    print(f"- 추가된 항목수: {len(added_items)}")
    print(f"- 결과 총 항목수: {len(base_list)}")


if __name__ == "__main__":
    main()
