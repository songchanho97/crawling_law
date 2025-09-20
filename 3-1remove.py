# -*- coding: utf-8 -*-
import re
import json
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional


file_name = "./data/산업안전보건법_시행령"

# ====================================
# 파일 경로
# ====================================
IN_XLSX = f"{file_name}_labeled.xlsx"      # 입력 엑셀
SHEET_NAME = None                                      # None이면 첫 시트 사용
OUT_XLSX = f"{file_name}_Ref_labeled_with_json.xlsx"  # 출력 엑셀

# ====================================
# 정규식 (코어 규칙과 동일)
# ====================================
CIRCLED_CHARS = [
    "①","②","③","④","⑤","⑥","⑦","⑧","⑨","⑩",
    "⑪","⑫","⑬","⑭","⑮","⑯","⑰","⑱","⑲","⑳"
]
CIRCLED_MAP = {ch: i+1 for i, ch in enumerate(CIRCLED_CHARS)}
CIRCLED_RE = re.compile("|".join(map(re.escape, CIRCLED_CHARS)))

# 제n조 / 제n조의m (제목 괄호는 옵션)
JOSA_RE = re.compile(
    r"^제\s*(\d+)(?:\s*조의\s*(\d+)|\s*조)(?:\(([^)]*)\))?",
    re.MULTILINE
)

# 문단 시작 '제 n 항' (보조 식별)
HANG_TEXT_RE = re.compile(r"(?m)^\s*제\s*(\d+)\s*항\b")

# 호: 줄머리 "1. ", "2. " ...
HO_LINE_RE = re.compile(r"(?m)^\s*(\d+)\.\s")

# 대괄호 메타 라인: [시행 ...] [법률 ...] ... 같은 라인은 버림
META_LINE_RE = re.compile(r"^(?:\s*\[[^\]]+\]\s*)+$")

# ====================================
# 유틸
# ====================================
def normalize_text(s: str) -> str:
    """줄바꿈/스페이스 정규화"""
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def extract_law_title_and_body(raw: str) -> Tuple[str, str]:
    """
    셀 원문에서 법제목과 본문 분리
    - 첫 비어있지 않은 줄 = 법제목 (예: 건설산업기본법)
    - 그 다음 이어지는 대괄호 메타 라인([시행...][법률...])들은 전부 건너뜀
    - 나머지 줄들을 본문으로 합침
    """
    if not isinstance(raw, str):
        return "", ""
    txt = normalize_text(raw)
    lines = [ln for ln in txt.split("\n")]
    # 제목 찾기
    title = ""
    i = 0
    while i < len(lines) and not title:
        cand = lines[i].strip()
        if cand:
            title = cand
        i += 1
    if not title:
        return "", ""

    # 메타 라인 스킵
    while i < len(lines) and META_LINE_RE.match(lines[i].strip() or ""):
        i += 1

    body = "\n".join(lines[i:]).strip()
    return title, body

def make_article_id(prefix: str, main_no: str, sub_no: Optional[str] = None) -> str:
    """제4조→prefix-4, 제4조의2→prefix-4_2"""
    return f"{prefix}-{main_no}_{sub_no}" if sub_no else f"{prefix}-{main_no}"

def make_article_number_field(main_no: str, sub_no: Optional[str] = None) -> str:
    """number 필드 '4' 또는 '4의2'"""
    return f"{main_no}의{sub_no}" if sub_no else str(main_no)

def split_by_articles(full_text: str) -> List[Tuple[str, Optional[str], str, int, int]]:
    """본문에서 조 단위 블록으로 분할."""
    matches = list(JOSA_RE.finditer(full_text))
    chunks: List[Tuple[str, Optional[str], str, int, int]] = []
    for i, m in enumerate(matches):
        main_no = m.group(1)
        sub_no  = m.group(2)  # None or '2'
        title   = m.group(3) or ""
        start   = m.start()
        end     = matches[i+1].start() if i+1 < len(matches) else len(full_text)
        chunks.append((main_no, sub_no, title, start, end))
    return chunks

def find_first_hang_start(block: str) -> int:
    """블록 내 첫 '항' 시작 위치(① 또는 '제n항') 인덱스. 없으면 -1"""
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

def find_hang_positions(block_text: str):
    """①②… 항 표식 위치 목록"""
    positions = []
    for m in CIRCLED_RE.finditer(block_text):
        sym = m.group(0)
        positions.append((m.start(), CIRCLED_MAP[sym], sym))
    positions.sort(key=lambda x: x[0])
    return positions

def split_hang_texts(block_text: str, hang_positions):
    """항 분리: [(항번호, 항 텍스트)] — 선두 ① 제거"""
    parts = []
    if not hang_positions:
        return parts
    for i, (pos, num, sym) in enumerate(hang_positions):
        start = pos
        end   = hang_positions[i+1][0] if i + 1 < len(hang_positions) else len(block_text)
        raw   = block_text[start:end].lstrip()
        if raw.startswith(sym):
            raw = raw[len(sym):].lstrip()
        parts.append((num, raw.rstrip()))
    return parts

def split_ho_with_preface(hang_text: str):
    """
    항 텍스트에서 '호' 분리
    - 반환1: 항 머리말(첫 '1.' 이전)
    - 반환2: [(호번호, 호 텍스트)]
    """
    matches = list(HO_LINE_RE.finditer(hang_text))
    if not matches:
        return hang_text.strip(), []
    preface_end = matches[0].start()
    preface = hang_text[:preface_end].strip()
    results = []
    for i, m in enumerate(matches):
        ho_no = int(m.group(1))
        start = m.start()
        end   = matches[i+1].start() if i + 1 < len(matches) else len(hang_text)
        piece = hang_text[start:end].strip()
        piece = re.sub(r"^\s*\d+\.\s*", "", piece)
        results.append((ho_no, piece.strip()))
    return preface, results

def hard_cut_article_text(article_text: str) -> str:
    """조.text에서 ① 또는 '제n항' 등장 시 그 이전까지만 유지"""
    m = CIRCLED_RE.search(article_text)
    if m:
        return article_text[:m.start()].rstrip()
    m2 = HANG_TEXT_RE.search(article_text)
    if m2:
        return article_text[:m2.start()].rstrip()
    return article_text.rstrip()

def build_nodes_for_cell(cell_text: str) -> List[Dict[str, Any]]:
    """
    엑셀 셀 하나(링크텍스트 클릭시 데이터)를 JSON 노드 리스트로 변환.
    '별표', '별지' 등 조문이 아닌 경우는 별도로 처리.
    """
    law_title, body = extract_law_title_and_body(cell_text)
    if not law_title:
        return []  # 비어있거나 제목을 못 찾으면 빈 리스트 반환

    nodes: List[Dict[str, Any]] = []
    node_map: Dict[str, Dict[str, Any]] = {}

    chunks = split_by_articles(body)

    # =================================================================
    # MODIFIED PART: '별표' 및 '별지' 처리 로직 추가
    # -----------------------------------------------------------------
    # '제n조' 패턴이 없고, 제목에 '별표' 또는 '별지'가 포함된 경우 특별 처리
    if not chunks and ("별표" in law_title or "별지" in law_title):
        node_id_text = law_title # 제목을 ID 및 텍스트로 사용
        special_node = {
            "id": node_id_text,
            "law_title": node_id_text,
            "level": "기타",
            "number": "기타",
            "parent_id": None,
            "Children_id": [],
            "text": node_id_text,
            "refs": []
        }
        return [special_node] # 단일 노드를 리스트에 담아 반환
    # =================================================================

    if not chunks:
        return []  # '제n조'가 없으면 생성 안 함

    for main_no, sub_no, title, start, end in chunks:
        block = body[start:end].strip()

        # 조 헤더
        m_head = JOSA_RE.match(block)
        header_txt = m_head.group(0).strip() if m_head else block.split("\n", 1)[0].strip()

        # 항 시작 위치
        first_hang_idx = find_first_hang_start(block)

        # 조 텍스트(헤더 + 프롤로그)
        if first_hang_idx != -1 and m_head:
            preface = block[m_head.end():first_hang_idx].strip()
            article_text = header_txt if not preface else (header_txt + "\n" + preface)
        else:
            article_text = block  # 항 자체가 없으면 전체 블록

        article_text = hard_cut_article_text(article_text)

        article_id = make_article_id(law_title, main_no, sub_no)
        number_field = make_article_number_field(main_no, sub_no)

        art_node = {
            "id": article_id,
            "law_title": law_title,
            "level": "조",
            "number": number_field,
            "parent_id": None,
            "Children_id": [],
            "text": article_text,
            "refs": []
        }
        nodes.append(art_node)
        node_map[article_id] = art_node

        # 항/호 분해: ①(또는 '제n항' 시작이 있더라도) 실제 항 표식(①…)이 없으면 항 분해 안 함
        if first_hang_idx == -1:
            continue

        after_header = block[first_hang_idx:]
        hang_positions = find_hang_positions(after_header)
        hang_parts = split_hang_texts(after_header, hang_positions)
        if not hang_parts:
            # 원형 숫자 항이 없으면 항 분해 생략
            continue

        for hang_no, hang_txt in hang_parts:
            hang_preface, ho_list = split_ho_with_preface(hang_txt)

            hang_id = f"{article_id}({hang_no})"
            hang_node = {
                "id": hang_id,
                "law_title": law_title,
                "level": "항",
                "number": str(hang_no),
                "parent_id": article_id,
                "Children_id": [],
                "text": hang_preface,
                "refs": []
            }
            nodes.append(hang_node)
            node_map[article_id]["Children_id"].append(hang_id)
            node_map[hang_id] = hang_node

            for ho_no, ho_txt in ho_list:
                ho_id = f"{hang_id}[{ho_no}]"
                ho_node = {
                    "id": ho_id,
                    "law_title": law_title,
                    "level": "호",
                    "number": str(ho_no),
                    "parent_id": hang_id,
                    "Children_id": [],
                    "text": ho_txt,
                    "refs": []
                }
                nodes.append(ho_node)
                node_map[hang_id]["Children_id"].append(ho_id)
                node_map[ho_id] = ho_node

    return nodes

# ====================================
# 실행: 엑셀 → JSON 컬럼 생성 → 저장
# ====================================
def read_excel_first_or_named(path: str, sheet_name=None) -> tuple[pd.DataFrame, str]:
    xls = pd.ExcelFile(path)
    chosen = sheet_name if (sheet_name and sheet_name in xls.sheet_names) else xls.sheet_names[0]
    print(f"[INFO] 시트 선택: {chosen}")
    df = pd.read_excel(xls, sheet_name=chosen, dtype=str)
    return df, chosen

def main():
    df, sheet = read_excel_first_or_named(IN_XLSX, SHEET_NAME)

    col_src = "링크텍스트 클릭시 데이터"
    if col_src not in df.columns:
        raise ValueError(f"입력 엑셀에 '{col_src}' 컬럼이 없습니다.")

    # 각 셀을 JSON으로 변환(compact하게 직렬화; 엑셀 셀 내 줄바꿈 방지)
    json_col = []
    for txt in df[col_src].fillna(""):
        nodes = build_nodes_for_cell(txt)
        json_str = json.dumps(nodes, ensure_ascii=False, separators=(",", ":"))
        json_col.append(json_str)

    df["링크데이터_JSON"] = json_col

    # 저장
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet)

    print(f"[OK] 저장 완료 → {OUT_XLSX}")
    print(f"총 행수: {len(df)}")

if __name__ == "__main__":
    main()