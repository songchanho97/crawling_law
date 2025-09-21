# -*- coding: utf-8 -*-
import re
import json
from typing import List, Dict, Any, Tuple, Optional

# ====================================
# 설정
# ====================================
# 메인 실행부에서 동적할당
LAW_TITLE = ""
LAW_PREFIX = ""

# 항(①~⑳) 매핑 및 정규식
CIRCLED_CHARS = [
    "①",
    "②",
    "③",
    "④",
    "⑤",
    "⑥",
    "⑦",
    "⑧",
    "⑨",
    "⑩",
    "⑪",
    "⑫",
    "⑬",
    "⑭",
    "⑮",
    "⑯",
    "⑰",
    "⑱",
    "⑲",
    "⑳",
]
CIRCLED_MAP = {ch: i + 1 for i, ch in enumerate(CIRCLED_CHARS)}
CIRCLED_RE = re.compile("|".join(map(re.escape, CIRCLED_CHARS)))

# 조(제n조/제n조의m) 패턴: (제목)은 선택
# group(1)=본조번호, group(2)=의번호(optional), group(3)=제목(optional)
JOSA_RE = re.compile(
    r"^제\s*(\d+)(?:\s*조의\s*(\d+)|\s*조)(?:\(([^)]*)\))?", re.MULTILINE
)

# 텍스트형 항 보조 식별자: 문단 시작에서 '제 n 항'
HANG_TEXT_RE = re.compile(r"(?m)^\s*제\s*(\d+)\s*항\b")

# 호: 문단 시작 '1. ', '2. ' …
HO_LINE_RE = re.compile(r"(?m)^\s*(\d+)\.\s")


# ====================================
# 유틸
# ====================================
def normalize_text(s: str) -> str:
    """줄바꿈/스페이스 정규화"""
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()


def make_article_id(main_no: str, sub_no: Optional[str] = None) -> str:
    """제4조→산업안전보건법시행규칙-4, 제4조의2→산업안전보건법시행규칙-4_2"""
    return f"{LAW_PREFIX}-{main_no}_{sub_no}" if sub_no else f"{LAW_PREFIX}-{main_no}"


def make_article_number_field(main_no: str, sub_no: Optional[str] = None) -> str:
    """number 필드: 4, 4의2"""
    return f"{main_no}의{sub_no}" if sub_no else str(main_no)


def split_by_articles(full_text: str) -> List[Tuple[str, Optional[str], str, int, int]]:
    """
    전체 문서를 조 단위 블록으로 분할.
    반환: [(본조번호, 의번호 or None, 제목, start, end)]
    """
    matches = list(JOSA_RE.finditer(full_text))
    chunks: List[Tuple[str, Optional[str], str, int, int]] = []
    for i, m in enumerate(matches):
        main_no = m.group(1)
        sub_no = m.group(2)  # None or '2'
        title = m.group(3) or ""
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(full_text)
        chunks.append((main_no, sub_no, title, start, end))
    return chunks


def find_first_hang_start(block: str) -> int:
    """
    블록 내 '항' 시작 위치(① 또는 문단 시작 '제n항')의 인덱스 반환. 없으면 -1
    """
    p1 = CIRCLED_RE.search(block)
    pos1 = p1.start() if p1 else -1
    p2 = HANG_TEXT_RE.search(block)
    pos2 = p2.start() if p2 else -1
    if pos1 == -1 and pos2 == -1:
        return -1
    if pos1 == -1:
        return pos2
    if pos2 == -1:
        return pos1
    return min(pos1, pos2)


def find_hang_positions(block_text: str) -> List[Tuple[int, int, str]]:
    """
    ①②… 항 위치 목록: [(index, 번호, 기호)]
    (텍스트형 '제n항'은 분할 기준에서 제외. 실제 파일은 ① 형태가 주류)
    """
    positions: List[Tuple[int, int, str]] = []
    for m in CIRCLED_RE.finditer(block_text):
        sym = m.group(0)
        positions.append((m.start(), CIRCLED_MAP[sym], sym))
    positions.sort(key=lambda x: x[0])
    return positions


def split_hang_texts(
    block_text: str, hang_positions: List[Tuple[int, int, str]]
) -> List[Tuple[int, str]]:
    """
    항 분리: [(항번호, 항 텍스트)]. 선두 ① 기호 제거.
    """
    parts: List[Tuple[int, str]] = []
    if not hang_positions:
        return parts
    for i, (pos, num, sym) in enumerate(hang_positions):
        start = pos
        end = (
            hang_positions[i + 1][0] if i + 1 < len(hang_positions) else len(block_text)
        )
        raw = block_text[start:end].lstrip()
        if raw.startswith(sym):
            raw = raw[len(sym) :].lstrip()
        parts.append((num, raw.rstrip()))
    return parts


def split_ho_with_preface(hang_text: str) -> Tuple[str, List[Tuple[int, str]]]:
    """
    항 텍스트에서 '호'를 분리하되,
    - 반환1: 항 머리말(첫 '1.' 이전 텍스트, 없으면 전체)
    - 반환2: [(호번호, 호 텍스트)] 목록
    """
    matches = list(HO_LINE_RE.finditer(hang_text))
    if not matches:
        # 호가 없으면 항 텍스트 그대로 머리말로 반환
        return hang_text.strip(), []

    preface_end = matches[0].start()
    preface = hang_text[:preface_end].strip()

    results: List[Tuple[int, str]] = []
    for i, m in enumerate(matches):
        ho_no = int(m.group(1))
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(hang_text)
        piece = hang_text[start:end].strip()
        piece = re.sub(r"^\s*\d+\.\s*", "", piece)  # '1. ' 제거
        results.append((ho_no, piece.strip()))
    return preface, results


def hard_cut_article_text(article_text: str) -> str:
    """
    조.text에 ① 또는 문단 시작 '제n항'이 들어있으면 강제 컷(이중 안전장치)
    """
    m = CIRCLED_RE.search(article_text)
    if m:
        return article_text[: m.start()].rstrip()
    m2 = HANG_TEXT_RE.search(article_text)
    if m2:
        return article_text[: m2.start()].rstrip()
    return article_text.rstrip()


# ====================================
# 메인 빌더
# ====================================
def build_nodes(full_text: str) -> List[Dict[str, Any]]:
    """
    규칙:
      - 조.text: '제n조(제목)' + (있다면 ① 이전 프롤로그까지만)
      - 항.text: '호' 목록 앞의 머리말만
      - 호.text: 각 '1. …' 항목 본문
      - 항(① 등)이 전혀 없으면 조만 생성(정의형 조 등)
      - refs는 항상 []
    """
    nodes: List[Dict[str, Any]] = []
    node_map: Dict[str, Dict[str, Any]] = {}

    for main_no, sub_no, title, start, end in split_by_articles(full_text):
        block = full_text[start:end].strip()

        # 조 헤더(제n조(제목)) 문자열
        m_head = JOSA_RE.match(block)
        header_txt = (
            m_head.group(0).strip() if m_head else block.split("\n", 1)[0].strip()
        )

        # 블록 내 첫 '항' 시작 위치(① 또는 문단 시작 '제n항')
        first_hang_idx = find_first_hang_start(block)

        # 조 텍스트(헤더 + ① 이전 프롤로그)
        if first_hang_idx != -1 and m_head:
            preface = block[m_head.end() : first_hang_idx].strip()
            article_text = header_txt if not preface else (header_txt + "\n" + preface)
        else:
            # 항 자체가 없으면 전체 블록(요청사항)
            article_text = block

        # 이중 안전장치: 조.text에서 ①/제n항 등장 시 무조건 컷
        article_text = hard_cut_article_text(article_text)

        # 조 노드 생성
        article_id = make_article_id(main_no, sub_no)
        number_field = make_article_number_field(main_no, sub_no)
        art_node = {
            "id": article_id,
            "law_title": LAW_TITLE,
            "level": "조",
            "number": number_field,
            "parent_id": None,
            "Children_id": [],
            "text": article_text,
            "refs": [],
        }
        nodes.append(art_node)
        node_map[article_id] = art_node

        # 항 분해: ① 없으면 종료
        if first_hang_idx == -1:
            continue

        # ① 이후만 잘라서 항 분해
        after_header = block[first_hang_idx:]
        hang_positions = find_hang_positions(after_header)
        hang_parts = split_hang_texts(after_header, hang_positions)

        for hang_no, hang_txt in hang_parts:
            # 항의 머리말/호 분리 (핵심 수정)
            hang_preface, ho_list = split_ho_with_preface(hang_txt)

            hang_id = f"{article_id}({hang_no})"
            hang_node = {
                "id": hang_id,
                "law_title": LAW_TITLE,
                "level": "항",
                "number": str(hang_no),
                "parent_id": article_id,
                "Children_id": [],
                "text": hang_preface,  # ✅ 항.text에는 머리말만
                "refs": [],
            }
            nodes.append(hang_node)
            node_map[article_id]["Children_id"].append(hang_id)
            node_map[hang_id] = hang_node

            # 호 분해
            for ho_no, ho_txt in ho_list:
                ho_id = f"{hang_id}[{ho_no}]"
                ho_node = {
                    "id": ho_id,
                    "law_title": LAW_TITLE,
                    "level": "호",
                    "number": str(ho_no),
                    "parent_id": hang_id,
                    "Children_id": [],
                    "text": ho_txt,
                    "refs": [],
                }
                nodes.append(ho_node)
                node_map[hang_id]["Children_id"].append(ho_id)
                node_map[ho_id] = ho_node

    return nodes


# ====================================
# 실행 (경로만 바꿔서 사용)
# ====================================
# 수정2
if __name__ == "__main__":
    # 🔽 수정 1: 처리할 파일 이름 목록을 여기에 추가합니다. (확장자 제외)
    file_names_to_process = [
        "해체공사표준안전작업지침",
        "추락재해방지표준안전작업지침",
        "유해·위험방지계획서 자체심사 및 확인업체 지정대상 건설업체 고시",  # 해당 데이터는 가져올 수 없음
        "보호구 자율안전확인 고시",
        "건설업 유해·위험방지계획서 중 지도사가 평가·확인 할 수 있는 대상 건설공사의 범위 및 지도사의 요건",
        "가설공사 표준안전 작업지침",
        "방호장치 안전인증 고시",
        "안전인증·자율안전확인신고의 절차에 관한 고시",
        "방호장치 자율안전기준 고시",
        "굴착공사 표준안전 작업지침",
        "위험기계·기구 안전인증 고시",
        "건설업체의 산업재해예방활동 실적 평가기준",
        "안전보건교육규정",
        "건설공사 안전보건대장의 작성 등에 관한 고시",
        "건설업 산업안전보건관리비 계상 및 사용기준",
        "산업재해예방시설자금 융자금 지원사업 및 클린사업장 조성지원사업 운영규정",
    ]

    # 🔽 수정 2: 반복문으로 각 파일을 순차적으로 처리합니다.
    for file_name in file_names_to_process:
        print(f"\n▶️ '{file_name}' 파일 처리 시작...")

        # 전역 변수인 LAW_TITLE과 LAW_PREFIX를 현재 파일명으로 동적 업데이트
        LAW_TITLE = file_name
        LAW_PREFIX = file_name

        input_path = f"./data/고시및예규/{file_name}_원문.txt"
        output_path = f"./data/고시및예규/{file_name}_큰틀.json"

        try:
            with open(input_path, "r", encoding="utf-8") as f:
                raw = f.read()

            text = normalize_text(raw)
            nodes = build_nodes(text)

            # 검증: 항.text에 '1.'이 남아있으면 경고
            bad_hang = [
                n["id"]
                for n in nodes
                if n["level"] == "항" and HO_LINE_RE.search(n["text"])
            ]
            if bad_hang:
                print(
                    f"[경고] '{file_name}' 처리 중, 일부 항.text에 '호'가 남아있을 수 있습니다. (노드: {bad_hang[:3]})"
                )

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(nodes, f, ensure_ascii=False, indent=2)

            print(f"✅ [완료] {len(nodes)}개 노드 저장 → {output_path}")

        except FileNotFoundError:
            print(f"❌ [오류] 입력 파일을 찾을 수 없습니다: {input_path}")
        except Exception as e:
            print(f"❌ [오류] '{file_name}' 처리 중 예외 발생: {e}")

    print("\n🎉 모든 파일 처리가 완료되었습니다.")
