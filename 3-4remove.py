"""
여러 '_merged.json' 파일의 중복을 제거하는 스크립트.

규칙:
1) 같은 id를 가진 항목 중 'refs'가 비어있지 않은 것을 우선하여 1개만 남깁니다.
2) 모든 중복 항목의 'refs'가 비어있다면, 가장 먼저 나온 항목을 남깁니다.
3) id가 없거나 형식이 맞지 않는 항목은 그대로 유지합니다.

- 처리할 파일 목록을 FILES_TO_PROCESS 리스트에 정의합니다.
- 각 파일에 대해 다음을 수행합니다:
  - 입력: {file_base}_merged.json
  - 출력: {file_base}_dedup.json
"""

import json
import os
from typing import Any, Dict, List

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
# 도우미 함수
# ====================================
def refs_nonempty(item: Dict[str, Any]) -> bool:
    """'refs' 필드가 비어있지 않은 리스트이면 True를 반환합니다."""
    if not isinstance(item, dict):
        return False
    refs = item.get("refs", [])
    return isinstance(refs, list) and len(refs) > 0


# ====================================
# 핵심 로직 함수
# ====================================
def deduplicate_json_file(file_base: str) -> Dict[str, Any]:
    """단일 JSON 파일의 중복을 제거하고 통계를 반환합니다."""
    in_path = f"{file_base}_merged.json"
    out_path = f"{file_base}_dedup.json"

    # 1) 입력 파일 확인
    if not os.path.exists(in_path):
        print(f"  [SKIP] 입력 파일 '{in_path}'을(를) 찾을 수 없습니다.")
        return {}

    # 2) 데이터 로드
    with open(in_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("입력 JSON의 루트는 리스트여야 합니다.")

    # 3) 중복 제거 로직
    kept: List[Any] = []
    idx_by_id: Dict[str, int] = {}
    has_nonempty_by_id: Dict[str, bool] = {}

    stats = {
        "replaced": 0,
        "skipped": 0,
        "orphans": 0,
        "total_in": len(data),
        "total_out": 0,
    }

    for item in data:
        if not isinstance(item, dict) or "id" not in item:
            kept.append(item)
            stats["orphans"] += 1
            continue

        _id = item["id"]
        cur_has_refs = refs_nonempty(item)

        if _id not in idx_by_id:
            # 최초 등장: 일단 저장
            kept.append(item)
            idx = len(kept) - 1
            idx_by_id[_id] = idx
            has_nonempty_by_id[_id] = cur_has_refs
        else:
            # 중복 등장: 교체 여부 판단
            idx = idx_by_id[_id]
            prev_has_refs = has_nonempty_by_id[_id]

            if not prev_has_refs and cur_has_refs:
                # 기존 항목(refs 없음)을 새 항목(refs 있음)으로 교체
                kept[idx] = item
                has_nonempty_by_id[_id] = True
                stats["replaced"] += 1
            else:
                # 기존 항목 유지 (기존에 refs가 있거나, 둘 다 refs가 없는 경우)
                stats["skipped"] += 1

    # 4) 결과 저장 및 통계 반환
    stats["total_out"] = len(kept)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(kept, f, ensure_ascii=False, indent=2)

    stats["out_path"] = out_path
    return stats


# ====================================
# 실행
# ====================================
def main():
    print("===== JSON 중복 제거 작업 시작 =====")
    grand_total_in = 0
    grand_total_out = 0

    for file_base in FILES_TO_PROCESS:
        file_disp_name = os.path.basename(file_base)
        print(f"\n▶️  '{file_disp_name}' 처리 시작...")
        try:
            result = deduplicate_json_file(file_base)
            if result:
                grand_total_in += result["total_in"]
                grand_total_out += result["total_out"]
                print(f"  ✅ 중복 제거 완료 → {result['out_path']}")
                print(f"    - 입력: {result['total_in']} → 출력: {result['total_out']}")
                print(
                    f"    - 교체: {result['replaced']}, 건너뜀: {result['skipped']}, 비정형: {result['orphans']}"
                )
        except Exception as e:
            print(f"  🚨 처리 중 오류 발생: {e}")

    print("\n" + "=" * 20 + " 모든 작업 완료 " + "=" * 20)
    print(f"총 {len(FILES_TO_PROCESS)}개 파일 처리 완료.")
    print(f"전체 입력 항목 수: {grand_total_in}")
    print(f"전체 출력 항목 수: {grand_total_out}")
    print(f"전체 제거된 항목 수: {grand_total_in - grand_total_out}")


if __name__ == "__main__":
    main()
