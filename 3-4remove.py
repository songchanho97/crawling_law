# -*- coding: utf-8 -*-
"""
산업안전보건법_merged.json 중복 제거 스크립트
규칙:
1) 같은 id 중 refs가 비어있지 않은 항목을 우선으로 1개만 남김
2) 모두 refs가 비어있다면 최초 등장 항목을 남김
출력: 산업안전보건법_dedup.json
"""

import json
from typing import Any, Dict, List


file_name = "./data/산업안전보건법_시행령"

IN_PATH = f"{file_name}_merged.json"
OUT_PATH = f"{file_name}_dedup.json"


def refs_nonempty(item: Dict[str, Any]) -> bool:
    """refs가 비어있지 않으면 True (없거나 리스트가 아니면 False)"""
    if not isinstance(item, dict):
        return False
    refs = item.get("refs", [])
    return isinstance(refs, list) and len(refs) > 0


def main():
    # 1) 입력 로드
    with open(IN_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("입력 JSON의 루트는 list여야 합니다.")

    # 2) id 기준 중복 제거
    kept: List[Any] = []
    idx_by_id: Dict[str, int] = {}  # id -> kept에서의 index
    has_nonempty_by_id: Dict[str, bool] = (
        {}
    )  # 그 id로 현재 보관된 항목이 refs 비었는지 여부

    # 통계
    total = len(data)
    replaced_empty_with_nonempty = 0
    skipped_duplicates = 0
    orphan_items = 0  # dict 아니거나 id 없는 항목

    for item in data:
        if not isinstance(item, dict) or "id" not in item:
            # 규격 밖 항목은 그대로 뒤에 붙임(손대지 않음)
            kept.append(item)
            orphan_items += 1
            continue

        _id = item["id"]
        cur_has_refs = refs_nonempty(item)

        if _id not in idx_by_id:
            # 최초 등장 → 일단 채택
            kept.append(item)
            idx = len(kept) - 1
            idx_by_id[_id] = idx
            has_nonempty_by_id[_id] = cur_has_refs
        else:
            # 기존에 동일 id가 이미 있음 → 교체 판단
            idx = idx_by_id[_id]
            prev_has_refs = has_nonempty_by_id[_id]

            if prev_has_refs:
                # 이미 refs가 있는 걸 보관 중이면 그대로 유지(가장 먼저 나온 refs-보유 항목)
                skipped_duplicates += 1
            else:
                # 현재 보관 중인 건 refs 없음
                if cur_has_refs:
                    # 새 항목에 refs가 있으므로 교체(동일 위치에 대체)
                    kept[idx] = item
                    has_nonempty_by_id[_id] = True
                    replaced_empty_with_nonempty += 1
                else:
                    # 둘 다 refs 없음 → 최초 것 유지
                    skipped_duplicates += 1

    # 3) 저장
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(kept, f, ensure_ascii=False, indent=2)

    # 4) 리포트
    print(f"[OK] 중복 제거 완료 → {OUT_PATH}")
    print(f"- 입력 총 항목수: {total}")
    print(f"- 결과 총 항목수: {len(kept)}")
    print(f"- 교체(빈 refs → 비지 않은 refs): {replaced_empty_with_nonempty}")
    print(f"- 건너뛴 중복 항목수(규칙상 유지): {skipped_duplicates}")
    print(f"- 비정형/무id 항목(그대로 유지): {orphan_items}")


if __name__ == "__main__":
    main()
