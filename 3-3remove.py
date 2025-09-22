# -*- coding: utf-8 -*-
"""
여러 엑셀 파일 각각에 대해, '링크데이터_JSON' 컬럼의 모든 항목을
기존 JSON 파일 뒤에 그대로 이어붙이는 스크립트.

- 처리할 파일 목록을 FILES_TO_PROCESS 리스트에 정의합니다.
- 각 파일 쌍에 대해 다음을 수행합니다:
  - 입력: {file_base}_refs_filled.json, {file_base}_Ref_labeled_with_json.xlsx
  - 출력: {file_base}_merged.json
- id 매칭, 구조 변경, 중복 제거 없이 단순히 데이터를 추가합니다.
"""

import json
import pandas as pd
import os
from typing import Any, List, Dict, Optional

# ====================================
# 처리할 파일 목록
# ====================================
# 여기에 처리할 파일의 기본 경로를 추가하세요.
# 예: "./data/산업안전보건법_시행령"
# --------------------------------------------------------------------------
FILES_TO_PROCESS = [
    "./data/고시및예규/해체공사표준안전작업지침",
    "./data/고시및예규/추락재해방지표준안전작업지침",
    "./data/고시및예규/유해·위험방지계획서 자체심사 및 확인업체 지정대상 건설업체 고시",
    "./data/고시및예규/보호구 자율안전확인 고시",
    "./data/고시및예규/건설업 유해·위험방지계획서 중 지도사가 평가·확인 할 수 있는 대상 건설공사의 범위 및 지도사의 요건",
    "./data/고시및예규/가설공사 표준안전 작업지침",
    "./data/고시및예규/방호장치 안전인증 고시",
    # "./data/안전인증·자율안전확인신고의 절차에 관한 고시",
    # "./data/방호장치 자율안전기준 고시",
    # "./data/굴착공사 표준안전 작업지침",
    # "./data/위험기계·기구 안전인증 고시",
    # "./data/건설업체의 산업재해예방활동 실적 평가기준",
    # "./data/안전보건교육규정",
    # "./data/건설공사 안전보건대장의 작성 등에 관한 고시",
    # "./data/건설업 산업안전보건관리비 계상 및 사용기준",
    # "./data/산업재해예방시설자금 융자금 지원사업 및 클린사업장 조성지원사업 운영규정",
]

# ====================================
# 전역 설정
# ====================================
LINK_JSON_COLS = ["링크데이터_JSON", "링크데이터_json"]  # 후보 컬럼명


# ====================================
# 유틸리티 함수 (기존과 동일)
# ====================================
def find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """후보 컬럼명 리스트 중 실제 존재하는 첫 번째 컬럼명 반환."""
    norm = {c.lower().replace(" ", ""): c for c in df.columns}
    for want in candidates:
        key = want.lower().replace(" ", "")
        if key in norm:
            return norm[key]
    return None


def safe_json_loads(cell: Any) -> Optional[Any]:
    """셀의 문자열을 JSON으로 안전하게 파싱."""
    if pd.isna(cell):
        return None
    if isinstance(cell, (list, dict)):
        return cell
    text = str(cell).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        try:
            return json.loads(text.replace("'", '"'))
        except Exception:
            return None


def flatten_link_json(val: Any) -> List[Dict[str, Any]]:
    """파싱된 JSON을 항상 리스트 형태로 반환."""
    parsed = safe_json_loads(val)
    if parsed is None:
        return []
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        return [parsed]
    return []


# ====================================
# 핵심 로직 함수
# ====================================
def merge_excel_to_json(file_base: str, sheet_name: Any = None) -> Dict[str, Any]:
    """단일 파일 쌍을 처리하여 JSON을 병합하고 통계를 반환합니다."""

    excel_path = f"{file_base}_Ref_labeled_with_json.xlsx"
    json_in_path = f"{file_base}_refs_filled.json"
    json_out_path = f"{file_base}_merged.json"

    # 1) 입력 파일 존재 확인
    if not os.path.exists(json_in_path) or not os.path.exists(excel_path):
        print(f"  [SKIP] 입력 파일(.json 또는 .xlsx)을 찾을 수 없습니다.")
        return {}

    # 2) 기존 JSON 로드
    with open(json_in_path, "r", encoding="utf-8") as f:
        base = json.load(f)

    base_list = base if isinstance(base, list) else [base]
    original_len = len(base_list)

    # 3) 엑셀 로드 및 컬럼 탐색
    df = pd.read_excel(excel_path, sheet_name=0, dtype=str)
    link_col = find_column(df, LINK_JSON_COLS)
    if not link_col:
        raise ValueError(f"엑셀에 '{LINK_JSON_COLS}' 중 유효한 컬럼이 없습니다.")

    # 4) 엑셀 데이터 수집 및 병합
    added_items: List[Any] = []
    nonempty_cells = 0
    for _, row in df.iterrows():
        items = flatten_link_json(row.get(link_col))
        if items:
            nonempty_cells += 1
            added_items.extend(items)

    base_list.extend(added_items)

    # 5) 저장 및 통계 반환
    with open(json_out_path, "w", encoding="utf-8") as f:
        json.dump(base_list, f, ensure_ascii=False, indent=2)

    return {
        "original_len": original_len,
        "total_cells": len(df),
        "nonempty_cells": nonempty_cells,
        "added_count": len(added_items),
        "final_len": len(base_list),
        "out_path": json_out_path,
    }


# ====================================
# 실행
# ====================================
def main():
    print("===== JSON 병합 작업 시작 =====")
    total_added_items = 0

    for file_base in FILES_TO_PROCESS:
        file_disp_name = os.path.basename(file_base)
        print(f"\n▶️  '{file_disp_name}' 처리 시작...")
        try:
            result = merge_excel_to_json(file_base)
            if result:
                total_added_items += result["added_count"]
                print(f"  ✅ 병합 완료 → {result['out_path']}")
                print(
                    f"    - 기존 항목: {result['original_len']}, 추가된 항목: {result['added_count']} → 최종: {result['final_len']}"
                )
        except Exception as e:
            print(f"  🚨 처리 중 오류 발생: {e}")

    print("\n" + "=" * 20 + " 모든 작업 완료 " + "=" * 20)
    print(f"총 {len(FILES_TO_PROCESS)}개 파일 처리 완료.")
    print(f"모든 파일에서 추가된 총 항목 수: {total_added_items}")


if __name__ == "__main__":
    main()
