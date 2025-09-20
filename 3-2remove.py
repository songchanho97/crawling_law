# -*- coding: utf-8 -*-
"""
엑셀의 id/링크텍스트/링크데이터_JSON을 이용해서
산업안전보건법.json의 각 노드.refs를 채우는 스크립트

- 매칭 기준: 엑셀 id == JSON node['id'] (정확 일치)
- label: '링크 텍스트' 우선, 비면 '링크 텍스트(원본)'
- 링크데이터_JSON: 배열/단일객체 모두 허용. 각 아이템에서
    - ref.id: item['id']
    - ref.law_title: item.get('law_title') or item.get('text') or id 기반 추정
    - ref.label: 위의 label
    - ref.relation: "" (빈 문자열)
- 매칭 노드 없음 행은 콘솔에 표시 + CSV 기록
"""

import json
import pandas as pd
import re
from typing import Any, Dict, List, Optional

file_name = "./data/산업안전보건법_시행령"

# ------------------------
# 경로 설정
# ------------------------
JSON_IN_PATH  = f"{file_name}_큰틀.json"
EXCEL_PATH    = f"{file_name}_Ref_labeled_with_json.xlsx"
SHEET_NAME    = 0  # 첫 시트 사용 (혹은 'law_final_data_with_hang_ho (1)')
JSON_OUT_PATH = f"{file_name}_refs_filled.json"
SKIPPED_NO_NODE_CSV = "rows_skipped_no_node.csv"
SKIPPED_EMPTY_LABEL_OR_JSON_CSV = "rows_skipped_empty_label_or_json.csv"

# ------------------------
# 도우미
# ------------------------
def ensure_list(obj) -> List:
    if obj is None:
        return []
    if isinstance(obj, list):
        return obj
    return [obj]

def pick_label(row: pd.Series) -> Optional[str]:
    """라벨은 '링크 텍스트' 우선, 없으면 '링크 텍스트(원본)'."""
    val1 = str(row.get("링크 텍스트", "") or "").strip()
    if val1:
        return val1
    val2 = str(row.get("링크 텍스트(원본)", "") or "").strip()
    return val2 if val2 else None

def parse_link_json(cell_value: Any) -> List[Dict[str, Any]]:
    """
    엑셀의 '링크데이터_JSON' 셀에서 JSON 파싱.
    - 문자열이면 json.loads
    - 리스트/딕셔너리도 수용
    - 실패/빈 값이면 []
    """
    if cell_value is None:
        return []
    if isinstance(cell_value, (list, dict)):
        return ensure_list(cell_value)
    text = str(cell_value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        # 셀에 작은 따옴표나 트레일링 콤마 등 있을 때 보정 시도(아주 간단)
        text2 = text.replace("'", '"')
        try:
            parsed = json.loads(text2)
        except Exception:
            return []
    return ensure_list(parsed)

def guess_law_title(item: Dict[str, Any]) -> str:
    """
    링크데이터 아이템에서 법명 추정:
      - item['law_title']가 있으면 그대로
      - 없으면 item['text'] 사용
      - 그래도 없으면 item['id']에서 접두부(법명-... 앞부분) 추출
    """
    lt = str(item.get("law_title", "") or "").strip()
    if lt:
        return lt
    tx = str(item.get("text", "") or "").strip()
    if tx:
        return tx
    rid = str(item.get("id", "") or "").strip()
    # id가 "산업안전보건법-10(1)[2]" 형태라면 '산업안전보건법'을 법명으로 추정
    m = re.match(r"^([^-]+)-", rid)
    return m.group(1) if m else (rid or "")

def ref_key_for_dedup(r: Dict[str, Any]) -> tuple:
    """중복 제거 키: (label, id, law_title)"""
    return (r.get("label", ""), r.get("id", ""), r.get("law_title", ""))

# ------------------------
# 메인
# ------------------------
def main():
    # 1) JSON 로드
    with open(JSON_IN_PATH, "r", encoding="utf-8") as f:
        nodes = json.load(f)

    id_to_idx: Dict[str, int] = {}
    for i, n in enumerate(nodes):
        nid = str(n.get("id", "")).strip()
        if nid:
            id_to_idx[nid] = i

    # 2) Excel 로드 (dtype=str로 원형 보존)
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, dtype=str)

    # 3) 처리
    updated_nodes = 0
    added_refs = 0
    skipped_no_node_rows = []
    skipped_empty_rows = []

    for r_idx, row in df.iterrows():
        nid = str(row.get("id", "") or "").strip()
        if not nid:
            continue  # id가 없으면 스킵

        label = pick_label(row)
        link_json = parse_link_json(row.get("링크데이터_JSON"))

        if not label or not link_json:
            # 라벨/링크데이터가 비어 있으면 통계용 수집 (추후 확인)
            skipped_empty_rows.append({
                "row_index": r_idx,
                "id": nid,
                "label": label,
                "링크데이터_JSON": str(row.get("링크데이터_JSON", ""))[:200]
            })
            continue

        idx = id_to_idx.get(nid)
        if idx is None:
            # 매칭 노드 없음 → 기록/출력
            print(f"[SKIP: no node] row={r_idx}, id={nid}, label={label}")
            skipped_no_node_rows.append({
                "row_index": r_idx,
                "id": nid,
                "label": label,
                "링크데이터_JSON": str(row.get("링크데이터_JSON", ""))[:200]
            })
            continue

        node = nodes[idx]
        if "refs" not in node or not isinstance(node["refs"], list):
            node["refs"] = []

        before = set(ref_key_for_dedup(x) for x in node["refs"])

        # 한 행의 링크데이터_JSON은 여러 대상일 수 있음(배열)
        to_add = []
        for item in link_json:
            target_id = str(item.get("id", "") or "").strip()
            if not target_id:
                continue
            law_title = guess_law_title(item)
            ref = {
                "label": label,
                "law_title": law_title,
                "id": target_id,
                "relation": ""
            }
            if ref_key_for_dedup(ref) not in before:
                to_add.append(ref)
                before.add(ref_key_for_dedup(ref))

        if to_add:
            node["refs"].extend(to_add)
            updated_nodes += 1
            added_refs += len(to_add)

    # 4) 결과 저장
    with open(JSON_OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(nodes, f, ensure_ascii=False, indent=2)

    if skipped_no_node_rows:
        pd.DataFrame(skipped_no_node_rows).to_csv(SKIPPED_NO_NODE_CSV, index=False, encoding="utf-8-sig")
    if skipped_empty_rows:
        pd.DataFrame(skipped_empty_rows).to_csv(SKIPPED_EMPTY_LABEL_OR_JSON_CSV, index=False, encoding="utf-8-sig")

    print(f"[OK] refs 채워 저장 → {JSON_OUT_PATH}")
    print(f"- refs 추가된 노드 수: {updated_nodes}")
    print(f"- 추가된 refs 총합: {added_refs}")
    print(f"- 매칭 노드 없음 행: {len(skipped_no_node_rows)} (CSV: {SKIPPED_NO_NODE_CSV if skipped_no_node_rows else '없음'})")
    print(f"- 빈 라벨/링크데이터 행: {len(skipped_empty_rows)} (CSV: {SKIPPED_EMPTY_LABEL_OR_JSON_CSV if skipped_empty_rows else '없음'})")

if __name__ == "__main__":
    main()
