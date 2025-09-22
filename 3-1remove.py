# -*- coding: utf-8 -*-
import re
import json
import os
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional

# ====================================
# 처리할 파일 목록
# ====================================
# 여기에 처리할 파일의 기본 경로를 추가하세요.
# 예: "./data/산업안전보건법_시행령"
# 입력 파일: {기본경로}_labeled.xlsx
# 출력 파일: {기본경로}_Ref_labeled_with_json.xlsx
# --------------------------------------------------------------------------
FILES_TO_PROCESS = [
    "./data/고시및예규/중대재해처벌법_시행령",
    "./data/고시및예규/산업안전보건법",
    "./data/고시및예규/화학물질관리법",
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
# 정규식 (기존과 동일)
# ====================================
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
JOSA_RE = re.compile(
    r"^제\s*(\d+)(?:\s*조의\s*(\d+)|\s*조)(?:\(([^)]*)\))?", re.MULTILINE
)
HANG_TEXT_RE = re.compile(r"(?m)^\s*제\s*(\d+)\s*항\b")
HO_LINE_RE = re.compile(r"(?m)^\s*(\d+)\.\s")
META_LINE_RE = re.compile(r"^(?:\s*\[[^\]]+\]\s*)+$")


# ====================================
# 유틸리티 및 파싱 함수 (기존과 동일)
# ====================================
def normalize_text(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()


def extract_law_title_and_body(raw: str) -> Tuple[str, str]:
    if not isinstance(raw, str):
        return "", ""
    txt = normalize_text(raw)
    lines = [ln for ln in txt.split("\n")]
    title = ""
    i = 0
    while i < len(lines) and not title:
        cand = lines[i].strip()
        if cand:
            title = cand
        i += 1
    if not title:
        return "", ""
    while i < len(lines) and META_LINE_RE.match(lines[i].strip() or ""):
        i += 1
    body = "\n".join(lines[i:]).strip()
    return title, body


def make_article_id(prefix: str, main_no: str, sub_no: Optional[str] = None) -> str:
    return f"{prefix}-{main_no}_{sub_no}" if sub_no else f"{prefix}-{main_no}"


def make_article_number_field(main_no: str, sub_no: Optional[str] = None) -> str:
    return f"{main_no}의{sub_no}" if sub_no else str(main_no)


def split_by_articles(full_text: str) -> List[Tuple[str, Optional[str], str, int, int]]:
    matches = list(JOSA_RE.finditer(full_text))
    chunks: List[Tuple[str, Optional[str], str, int, int]] = []
    for i, m in enumerate(matches):
        main_no = m.group(1)
        sub_no = m.group(2)
        title = m.group(3) or ""
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(full_text)
        chunks.append((main_no, sub_no, title, start, end))
    return chunks


def find_first_hang_start(block: str) -> int:
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
    positions = []
    for m in CIRCLED_RE.finditer(block_text):
        sym = m.group(0)
        positions.append((m.start(), CIRCLED_MAP[sym], sym))
    positions.sort(key=lambda x: x[0])
    return positions


def split_hang_texts(block_text: str, hang_positions):
    parts = []
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


def split_ho_with_preface(hang_text: str):
    matches = list(HO_LINE_RE.finditer(hang_text))
    if not matches:
        return hang_text.strip(), []
    preface_end = matches[0].start()
    preface = hang_text[:preface_end].strip()
    results = []
    for i, m in enumerate(matches):
        ho_no = int(m.group(1))
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(hang_text)
        piece = hang_text[start:end].strip()
        piece = re.sub(r"^\s*\d+\.\s*", "", piece)
        results.append((ho_no, piece.strip()))
    return preface, results


def hard_cut_article_text(article_text: str) -> str:
    m = CIRCLED_RE.search(article_text)
    if m:
        return article_text[: m.start()].rstrip()
    m2 = HANG_TEXT_RE.search(article_text)
    if m2:
        return article_text[: m2.start()].rstrip()
    return article_text.rstrip()


def build_nodes_for_cell(cell_text: str) -> List[Dict[str, Any]]:
    law_title, body = extract_law_title_and_body(cell_text)
    if not law_title:
        return []

    nodes: List[Dict[str, Any]] = []
    node_map: Dict[str, Dict[str, Any]] = {}
    chunks = split_by_articles(body)

    if not chunks and ("별표" in law_title or "별지" in law_title):
        node_id_text = law_title
        special_node = {
            "id": node_id_text,
            "law_title": node_id_text,
            "level": "기타",
            "number": "기타",
            "parent_id": None,
            "Children_id": [],
            "text": node_id_text,
            "refs": [],
        }
        return [special_node]

    if not chunks:
        return []

    for main_no, sub_no, title, start, end in chunks:
        block = body[start:end].strip()
        m_head = JOSA_RE.match(block)
        header_txt = (
            m_head.group(0).strip() if m_head else block.split("\n", 1)[0].strip()
        )
        first_hang_idx = find_first_hang_start(block)

        if first_hang_idx != -1 and m_head:
            preface = block[m_head.end() : first_hang_idx].strip()
            article_text = header_txt if not preface else (header_txt + "\n" + preface)
        else:
            article_text = block

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
            "refs": [],
        }
        nodes.append(art_node)
        node_map[article_id] = art_node

        if first_hang_idx == -1:
            continue

        after_header = block[first_hang_idx:]
        hang_positions = find_hang_positions(after_header)
        hang_parts = split_hang_texts(after_header, hang_positions)
        if not hang_parts:
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
                "refs": [],
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
                    "refs": [],
                }
                nodes.append(ho_node)
                node_map[hang_id]["Children_id"].append(ho_id)
                node_map[ho_id] = ho_node

    return nodes


def read_excel_first_or_named(path: str, sheet_name=None) -> tuple[pd.DataFrame, str]:
    xls = pd.ExcelFile(path)
    chosen = (
        sheet_name
        if (sheet_name and sheet_name in xls.sheet_names)
        else xls.sheet_names[0]
    )
    print(f"  [INFO] 시트 선택: {chosen}")
    df = pd.read_excel(xls, sheet_name=chosen, dtype=str)
    return df, chosen


# ====================================
# 핵심 로직 함수
# ====================================
def process_single_file(file_base: str, sheet_name: Optional[str] = None):
    """단일 파일을 읽어 JSON 컬럼을 추가하고 저장하는 함수"""
    in_xlsx = f"{file_base}_labeled.xlsx"
    out_xlsx = f"{file_base}_Ref_labeled_with_json.xlsx"

    # 입력 파일 확인
    if not os.path.exists(in_xlsx):
        print(f"[SKIP] 입력 파일을 찾을 수 없습니다: {in_xlsx}\n")
        return

    print(f"▶️  '{os.path.basename(file_base)}' 파일 처리 시작...")

    df, sheet = read_excel_first_or_named(in_xlsx, sheet_name)

    col_src = "링크텍스트 클릭시 데이터"
    if col_src not in df.columns:
        raise ValueError(f"입력 엑셀에 '{col_src}' 컬럼이 없습니다.")

    json_col = []
    for txt in df[col_src].fillna(""):
        nodes = build_nodes_for_cell(txt)
        json_str = json.dumps(nodes, ensure_ascii=False, separators=(",", ":"))
        json_col.append(json_str)

    df["링크데이터_JSON"] = json_col

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet)

    print(f"✅ 저장 완료 → {out_xlsx}")
    print(f"  총 행수: {len(df)}\n")


# ====================================
# 실행
# ====================================
if __name__ == "__main__":
    print("===== JSON 변환 작업 시작 =====")
    for file_base_path in FILES_TO_PROCESS:
        try:
            process_single_file(file_base_path)
        except Exception as e:
            file_name = os.path.basename(file_base_path)
            print(f"🚨 '{file_name}' 처리 중 오류 발생!")
            print(f"  오류 내용: {e}\n")
    print("===== 모든 작업 완료 =====")
