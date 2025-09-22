"""
여러 법령 JSON 파일에 대해, 각각에 해당하는 엑셀 파일을 읽어
JSON 노드의 'refs' 필드를 채우는 스크립트.

- 처리할 파일 목록을 FILES_TO_PROCESS 리스트에 정의합니다.
- 각 파일 쌍에 대해 다음을 수행합니다:
  - 입력: {file_base}_큰틀.json, {file_base}_Ref_labeled_with_json.xlsx
  - 출력: {file_base}_refs_filled.json
- 모든 파일 처리 후, 건너뛴 행들의 목록을 통합된 CSV 파일로 저장합니다.
"""

import json
import pandas as pd
import re
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
# 통합 로그 CSV 파일명
SKIPPED_NO_NODE_CSV = "all_rows_skipped_no_node.csv"
SKIPPED_EMPTY_LABEL_OR_JSON_CSV = "all_rows_skipped_empty_label_or_json.csv"


# ====================================
# 도우미 함수 (기존과 동일)
# ====================================
def ensure_list(obj) -> List:
    if obj is None:
        return []
    if isinstance(obj, list):
        return obj
    return [obj]


def pick_label(row: pd.Series) -> Optional[str]:
    val1 = str(row.get("링크 텍스트", "") or "").strip()
    if val1:
        return val1
    val2 = str(row.get("링크 텍스트(원본)", "") or "").strip()
    return val2 if val2 else None


def parse_link_json(cell_value: Any) -> List[Dict[str, Any]]:
    if cell_value is None:
        return []
    if isinstance(cell_value, (list, dict)):
        return ensure_list(cell_value)
    text = str(cell_value).strip()
    if not text:
        return []
    try:
        return ensure_list(json.loads(text))
    except json.JSONDecodeError:
        try:
            return ensure_list(json.loads(text.replace("'", '"')))
        except Exception:
            return []


def guess_law_title(item: Dict[str, Any]) -> str:
    lt = str(item.get("law_title", "") or "").strip()
    if lt:
        return lt
    tx = str(item.get("text", "") or "").strip()
    if tx:
        return tx
    rid = str(item.get("id", "") or "").strip()
    m = re.match(r"^([^-]+)-", rid)
    return m.group(1) if m else (rid or "")


def ref_key_for_dedup(r: Dict[str, Any]) -> tuple:
    return (r.get("label", ""), r.get("id", ""), r.get("law_title", ""))


# ====================================
# 핵심 로직 함수
# ====================================
def process_file(file_base: str, sheet_name: Any = 0) -> Dict[str, Any]:
    """단일 파일 쌍(JSON, Excel)을 처리하여 refs를 채우고 통계를 반환합니다."""

    json_in_path = f"{file_base}_큰틀.json"
    excel_path = f"{file_base}_Ref_labeled_with_json.xlsx"
    json_out_path = f"{file_base}_refs_filled.json"

    # 1) 입력 파일 존재 확인
    if not os.path.exists(json_in_path) or not os.path.exists(excel_path):
        print(f"  [SKIP] 입력 파일(.json 또는 .xlsx)을 찾을 수 없습니다.")
        return {}

    # 2) JSON 및 Excel 로드
    with open(json_in_path, "r", encoding="utf-8") as f:
        nodes = json.load(f)

    id_to_idx: Dict[str, int] = {
        str(n.get("id", "")).strip(): i
        for i, n in enumerate(nodes)
        if str(n.get("id", "")).strip()
    }

    df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype=str)

    # 3) 처리
    updated_nodes = 0
    added_refs = 0
    skipped_no_node_rows = []
    skipped_empty_rows = []

    for r_idx, row in df.iterrows():
        nid = str(row.get("id", "") or "").strip()
        if not nid:
            continue

        label = pick_label(row)
        link_json = parse_link_json(row.get("링크데이터_JSON"))

        if not label or not link_json:
            skipped_empty_rows.append(
                {
                    "source_file": os.path.basename(file_base),
                    "row_index": r_idx,
                    "id": nid,
                    "label": label,
                    "json_preview": str(row.get("링크데이터_JSON", ""))[:100],
                }
            )
            continue

        idx = id_to_idx.get(nid)
        if idx is None:
            skipped_no_node_rows.append(
                {
                    "source_file": os.path.basename(file_base),
                    "row_index": r_idx,
                    "id": nid,
                    "label": label,
                    "json_preview": str(row.get("링크데이터_JSON", ""))[:100],
                }
            )
            continue

        node = nodes[idx]
        if "refs" not in node or not isinstance(node["refs"], list):
            node["refs"] = []

        before_keys = {ref_key_for_dedup(x) for x in node["refs"]}

        to_add = []
        for item in link_json:
            target_id = str(item.get("id", "") or "").strip()
            if not target_id:
                continue

            ref = {
                "label": label,
                "law_title": guess_law_title(item),
                "id": target_id,
                "relation": "",
            }

            ref_key = ref_key_for_dedup(ref)
            if ref_key not in before_keys:
                to_add.append(ref)
                before_keys.add(ref_key)

        if to_add:
            node["refs"].extend(to_add)
            updated_nodes += 1
            added_refs += len(to_add)

    # 4) 결과 저장 및 통계 반환
    with open(json_out_path, "w", encoding="utf-8") as f:
        json.dump(nodes, f, ensure_ascii=False, indent=2)

    return {
        "updated_nodes": updated_nodes,
        "added_refs": added_refs,
        "skipped_no_node": skipped_no_node_rows,
        "skipped_empty": skipped_empty_rows,
        "out_path": json_out_path,
    }


# ====================================
# 실행
# ====================================
def main():
    print("===== Refs 채우기 작업 시작 =====")
    total_stats = {"updated": 0, "added": 0}
    all_skipped_no_node = []
    all_skipped_empty = []

    for file_base in FILES_TO_PROCESS:
        file_disp_name = os.path.basename(file_base)
        print(f"\n▶️  '{file_disp_name}' 처리 시작...")
        try:
            result = process_file(file_base)
            if result:
                total_stats["updated"] += result["updated_nodes"]
                total_stats["added"] += result["added_refs"]
                all_skipped_no_node.extend(result["skipped_no_node"])
                all_skipped_empty.extend(result["skipped_empty"])
                print(f"  ✅ 저장 완료 → {result['out_path']}")
                print(
                    f"    - Refs 추가된 노드: {result['updated_nodes']}, 추가된 refs 총합: {result['added_refs']}"
                )
        except Exception as e:
            print(f"  🚨 처리 중 오류 발생: {e}")

    # 최종 요약 및 통합 CSV 저장
    print("\n" + "=" * 20 + " 모든 작업 완료 " + "=" * 20)
    print(f"- 총 refs 추가된 노드 수: {total_stats['updated']}")
    print(f"- 총 추가된 refs 합계: {total_stats['added']}")

    if all_skipped_no_node:
        pd.DataFrame(all_skipped_no_node).to_csv(
            SKIPPED_NO_NODE_CSV, index=False, encoding="utf-8-sig"
        )
        print(
            f"- 매칭 노드 없음 행: {len(all_skipped_no_node)} (CSV 저장: {SKIPPED_NO_NODE_CSV})"
        )
    else:
        print("- 매칭 노드 없음 행: 0")

    if all_skipped_empty:
        pd.DataFrame(all_skipped_empty).to_csv(
            SKIPPED_EMPTY_LABEL_OR_JSON_CSV, index=False, encoding="utf-8-sig"
        )
        print(
            f"- 빈 라벨/링크데이터 행: {len(all_skipped_empty)} (CSV 저장: {SKIPPED_EMPTY_LABEL_OR_JSON_CSV})"
        )
    else:
        print("- 빈 라벨/링크데이터 행: 0")


if __name__ == "__main__":
    main()
