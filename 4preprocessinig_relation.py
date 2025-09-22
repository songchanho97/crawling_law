"""
여러 '_dedup.json' 파일을 읽어, 각 파일의 'refs' 정보를 바탕으로
엑셀 파일을 생성하는 스크립트.

동작:
 1) 'refs'가 비어있지 않은 노드만 대상으로, 각 ref.id를 이용해 대상 노드의
    텍스트를 같은 JSON 파일 내에서 찾아 붙입니다.
 2) 동일 출처(src) 노드 내에서 'ref_label'이 중복될 경우, 첫 번째 항목만 남깁니다.

- 처리할 파일 목록을 FILES_TO_PROCESS 리스트에 정의합니다.
- 각 파일에 대해 다음을 수행합니다:
  - 입력: {file_base}_dedup.json
  - 출력: {file_base}_refs_from_json_dedup.xlsx
"""

import json
import pandas as pd
import os
from typing import Any, Dict, List, Optional

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
TRUNCATE_SRC_TEXT = None
TRUNCATE_REF_TEXT = None
CASE_INSENSITIVE_LABEL = True


# ====================================
# 유틸리티 함수 (기존과 동일)
# ====================================
def truncate_text(s: Optional[str], limit: Optional[int]) -> str:
    if s is None:
        return ""
    s = str(s)
    if limit is None or limit <= 0 or len(s) <= limit:
        return s
    return s[:limit] + " …"


def refs_is_nonempty(node: Dict[str, Any]) -> bool:
    r = node.get("refs", [])
    return isinstance(r, list) and len(r) > 0


def node_text(node: Dict[str, Any]) -> str:
    return str(node.get("text", "") or "")


def get_ref_label(ref: Dict[str, Any]) -> str:
    return str(ref.get("label", "") or "")


def get_ref_law_title(ref: Dict[str, Any]) -> str:
    return str(ref.get("law_title", ref.get("lab_title", "")) or "")


def label_norm(label: str) -> str:
    return label.strip().lower() if CASE_INSENSITIVE_LABEL else label


# ====================================
# 핵심 로직 함수
# ====================================
def create_excel_from_json(file_base: str) -> Dict[str, Any]:
    """단일 JSON 파일을 처리하여 엑셀을 생성하고 통계를 반환합니다."""
    input_json = f"{file_base}_dedup.json"
    output_xlsx = f"{file_base}_refs_from_json_dedup.xlsx"

    # 1) 입력 파일 확인
    if not os.path.exists(input_json):
        print(f"  [SKIP] 입력 파일 '{input_json}'을(를) 찾을 수 없습니다.")
        return {}

    # 2) 데이터 로드 및 id-node 매핑
    with open(input_json, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("입력 JSON의 루트는 리스트여야 합니다.")

    id2node: Dict[str, Dict[str, Any]] = {
        str(n.get("id", "")): n for n in data if isinstance(n, dict) and n.get("id")
    }

    # 3) 엑셀 데이터(rows) 생성
    rows: List[Dict[str, Any]] = []
    for node in data:
        if not isinstance(node, dict) or not refs_is_nonempty(node):
            continue

        src_id = str(node.get("id", ""))
        src_law_title = str(node.get("law_title", ""))
        src_level = str(node.get("level", ""))
        src_number = str(node.get("number", ""))
        src_text_out = truncate_text(node_text(node), TRUNCATE_SRC_TEXT)

        for idx, ref in enumerate(node.get("refs", []), start=1):
            ref_id = str(ref.get("id", ""))
            target = id2node.get(ref_id)
            ref_text_out = (
                truncate_text(node_text(target), TRUNCATE_REF_TEXT) if target else ""
            )

            rows.append(
                {
                    "src_id": src_id,
                    "src_law_title": src_law_title,
                    "src_level": src_level,
                    "src_number": src_number,
                    "src_text": src_text_out,
                    "ref_index": idx,
                    "ref_label": get_ref_label(ref),
                    "ref_law_title": get_ref_law_title(ref),
                    "ref_id": ref_id,
                    "ref_text": ref_text_out,
                    "ref_found": target is not None,
                }
            )

    # 4) DataFrame 생성 및 중복 제거
    if not rows:
        # 처리할 데이터가 없는 경우 빈 엑셀 생성
        pd.DataFrame().to_excel(output_xlsx, index=False)
        return {"before_dedup": 0, "after_dedup": 0, "out_path": output_xlsx}

    df = pd.DataFrame(rows)
    df["_label_norm"] = df["ref_label"].fillna("").astype(str).apply(label_norm)
    key_cols = [
        "src_id",
        "src_law_title",
        "src_level",
        "src_number",
        "src_text",
        "_label_norm",
    ]

    before_dedup = len(df)
    df = df.drop_duplicates(subset=key_cols, keep="first").drop(columns=["_label_norm"])
    after_dedup = len(df)

    # 5) 저장 및 통계 반환
    with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="refs_lookup_dedup", index=False)

    return {
        "before_dedup": before_dedup,
        "after_dedup": after_dedup,
        "out_path": output_xlsx,
    }


# ====================================
# 실행
# ====================================
def main():
    print("===== JSON to Excel Export 작업 시작 =====")
    total_rows_generated = 0

    for file_base in FILES_TO_PROCESS:
        file_disp_name = os.path.basename(file_base)
        print(f"\n▶️  '{file_disp_name}' 처리 시작...")
        try:
            result = create_excel_from_json(file_base)
            if result:
                total_rows_generated += result["after_dedup"]
                print(f"  ✅ 저장 완료 → {result['out_path']}")
                print(
                    f"    - 중복 제거 전: {result['before_dedup']} 행 → 최종: {result['after_dedup']} 행"
                )
        except Exception as e:
            print(f"  🚨 처리 중 오류 발생: {e}")

    print("\n" + "=" * 20 + " 모든 작업 완료 " + "=" * 20)
    print(f"총 {len(FILES_TO_PROCESS)}개 파일 처리 완료.")
    print(f"모든 엑셀 파일에 생성된 총 행 수: {total_rows_generated}")


if __name__ == "__main__":
    main()
