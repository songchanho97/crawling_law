import re
import json
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
import csv
from collections import defaultdict

# ------------ CSV 로딩 보강(인코딩/구분자 자동 시도) ------------
CANDIDATE_ENCODINGS = ["utf-8", "utf-8-sig", "cp949", "euc-kr", "latin1"]
CANDIDATE_SEPARATORS = [
    None,
    ",",
    "\t",
    ";",
    "|",
]  # None=구분자 자동추정 (python engine 필요)


def read_csv_safely(path: str, **kwargs) -> pd.DataFrame:
    last_err = None
    for enc in CANDIDATE_ENCODINGS:
        for sep in CANDIDATE_SEPARATORS:
            try:
                df = pd.read_csv(
                    path,
                    encoding=enc,
                    sep=sep,
                    engine="python",
                    dtype=str,
                    quoting=csv.QUOTE_MINIMAL,
                    **kwargs,
                )
                # 최소 컬럼 검사: 조/링크 텍스트가 없으면 다음 시도
                if not {"조", "링크 텍스트"}.issubset(set(df.columns)):
                    last_err = ValueError(
                        f"구분자/인코딩 추정 실패(컬럼 누락): enc={enc}, sep={sep}"
                    )
                    continue
                print(
                    f"[INFO] CSV 인코딩={enc}, sep={'auto' if sep is None else repr(sep)}"
                )
                return df
            except Exception as e:
                last_err = e
                continue
    raise last_err


# ------------ 정규화 유틸 ------------
STRIP_CHARS = "「」[](){}〈〉《》【】'\"“”‘’·ㆍ,.;:"

WS_RE = re.compile(r"\s+")


def norm_for_match(s: str) -> str:
    """매칭용 정규화: 괄호류/구두점 제거 + 공백 제거"""
    if s is None:
        return ""
    t = s
    for ch in STRIP_CHARS:
        t = t.replace(ch, "")
    t = WS_RE.sub("", t)
    return t


def canonicalize_article_key(val: str) -> str:
    """
    CSV '조' 값을 정규화:
    - '4_2' 유지(우선)
    - '4의2' 유지
    - 숫자처럼 보이면 '정수문자열'로
    """
    if val is None:
        return ""
    s = str(val).strip()
    if "_" in s or "의" in s:
        return s
    # '2.0' -> '2'
    if re.fullmatch(r"\d+(?:\.0+)?", s):
        return str(int(float(s)))
    return s


# ------------ JSON → 조/항/호 인덱스 구축 ------------
def load_law_json(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("JSON 최상위는 list여야 합니다.")
    return data


def build_article_index(nodes: List[Dict[str, Any]]):
    """
    JSON 노드(조/항/호)를 이용해 매칭용 인덱스를 만든다.
    반환:
      articles_by_key: dict[str, dict]  # '4', '4의2', '4_2' 등으로 접근
      cursors: dict[str, {'seg_idx':0,'offset':0}]  # 조별 스캔 커서
    각 article 구조:
      {
        'id': '산업안전보건법-4_2',
        'number': '4의2',
        'underscore': '4_2',
        'base': '4',
        'segments': [
            {'scope':'조','hang':None,'ho':None,'text':..., 'text_norm':...},  # 조 본문이 있을 때만
            {'scope':'항','hang':1,'ho':None,'text':..., 'text_norm':...},
            {'scope':'호','hang':1,'ho':1,  'text':..., 'text_norm':...},
            ...
        ]
      }
    """
    nodes_by_id = {n["id"]: n for n in nodes}
    # 조 목록만 추출
    articles = [n for n in nodes if n.get("level") == "조"]

    def number_to_underscore(num: str) -> str:
        # '4의2' → '4_2'
        m = re.fullmatch(r"(\d+)(?:의(\d+))?", num)
        if not m:
            return num
        base, sub = m.group(1), m.group(2)
        return f"{base}_{sub}" if sub else base

    def number_base(num: str) -> str:
        m = re.fullmatch(r"(\d+)(?:의(\d+))?", num)
        return m.group(1) if m else num

    articles_by_key: Dict[str, Dict[str, Any]] = {}
    base_buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for a in articles:
        aid = a["id"]
        num = a.get("number", "")
        underscore = number_to_underscore(num)
        base = number_base(num)

        # 세그먼트 구성 (조 본문이 있으면 넣기)
        segments: List[Dict[str, Any]] = []
        a_text = (a.get("text") or "").strip()
        if a_text and a_text not in ("", None):
            segments.append(
                {
                    "scope": "조",
                    "hang": None,
                    "ho": None,
                    "text": a_text,
                    "text_norm": norm_for_match(a_text),
                }
            )

        # 항/호 추가: Children_id 순회(기존 순서 유지)
        for hid in a.get("Children_id", []) or []:
            hnode = nodes_by_id.get(hid)
            if not hnode or hnode.get("level") != "항":
                continue
            h_txt = (hnode.get("text") or "").strip()
            h_no = (
                int(hnode.get("number"))
                if str(hnode.get("number", "")).isdigit()
                else None
            )
            if h_txt:
                segments.append(
                    {
                        "scope": "항",
                        "hang": h_no,
                        "ho": None,
                        "text": h_txt,
                        "text_norm": norm_for_match(h_txt),
                    }
                )
            # 호
            for oid in hnode.get("Children_id", []) or []:
                onode = nodes_by_id.get(oid)
                if not onode or onode.get("level") != "호":
                    continue
                o_txt = (onode.get("text") or "").strip()
                o_no = (
                    int(onode.get("number"))
                    if str(onode.get("number", "")).isdigit()
                    else None
                )
                if o_txt:
                    segments.append(
                        {
                            "scope": "호",
                            "hang": h_no,
                            "ho": o_no,
                            "text": o_txt,
                            "text_norm": norm_for_match(o_txt),
                        }
                    )

        article = {
            "id": aid,
            "number": num,  # 예: '4의2'
            "underscore": underscore,  # 예: '4_2'
            "base": base,  # 예: '4'
            "segments": segments,
        }

        # 키 등록: '4의2', '4_2' 둘 다 접근 가능하게
        articles_by_key[num] = article
        articles_by_key[underscore] = article
        base_buckets[base].append(article)

    # 조별 커서
    cursors = {
        art["underscore"]: {"seg_idx": 0, "offset": 0}
        for art in {v["underscore"]: v for v in articles_by_key.values()}.values()
    }

    return articles_by_key, base_buckets, cursors


# ------------ 매칭 로직(조별 커서) ------------
def match_with_cursor(article: Dict[str, Any], link_text: str, cursor: Dict[str, int]):
    """
    article['segments']에서 link_text(정규화)를 cursor부터 순차 검색.
    반환: (hang, ho, scope, new_cursor)
    scope ∈ {'호','항','조','미검출'}
    """
    segs = article.get("segments", [])
    if not segs or not link_text or not norm_for_match(link_text):
        return None, None, "미검출", cursor

    q = norm_for_match(link_text)
    seg_idx = cursor.get("seg_idx", 0)
    offset = cursor.get("offset", 0)

    # 1패스: cursor→끝
    for i in range(seg_idx, len(segs)):
        s = segs[i]
        pos = s["text_norm"].find(q, offset if i == seg_idx else 0)
        if pos >= 0:
            new_off = pos + len(q)
            if new_off >= len(s["text_norm"]):
                new_cursor = {"seg_idx": i + 1, "offset": 0}
            else:
                new_cursor = {"seg_idx": i, "offset": new_off}
            return (s.get("hang"), s.get("ho"), s["scope"], new_cursor)

    # 2패스: 처음→끝 (안전장치)
    for i in range(0, len(segs)):
        s = segs[i]
        pos = s["text_norm"].find(q)
        if pos >= 0:
            new_off = pos + len(q)
            if new_off >= len(s["text_norm"]):
                new_cursor = {"seg_idx": i + 1, "offset": 0}
            else:
                new_cursor = {"seg_idx": i, "offset": new_off}
            return (s.get("hang"), s.get("ho"), s["scope"], new_cursor)

    return None, None, "미검출", cursor


# ------------ 메인 실행 로직을 함수로 전환 ------------
def process_law_file(file_name: str):
    """하나의 법령 파일 세트(json, csv)를 처리하여 결과 csv를 저장하는 함수"""
    print(f"\n▶️ '{file_name}' 파일 처리 시작...")

    # ------------ 경로 설정 (함수 내부로 이동) ------------
    JSON_PATH = f"./data/고시및예규/{file_name}_큰틀.json"
    CSV_PATH = f"./data/고시및예규/{file_name}_data.csv"
    OUT_CSV_PATH = f"./data/고시및예규/{file_name}_항_호.csv"

    # 1) JSON 읽기
    nodes = load_law_json(JSON_PATH)
    articles_by_key, base_buckets, cursors = build_article_index(nodes)
    print(
        f" INFO: 조(기사) 개수: {len({a['underscore'] for a in articles_by_key.values()})}"
    )

    # 2) CSV 읽기
    df = read_csv_safely(CSV_PATH)
    if not {"조", "링크 텍스트"}.issubset(set(df.columns)):
        raise ValueError("CSV에 '조', '링크 텍스트' 컬럼이 필요합니다.")

    # 3) 행별 매칭
    hang_col: List[Optional[int]] = []
    ho_col: List[Optional[int]] = []
    scope_col: List[str] = []
    matched_article_num: List[str] = []

    for _, row in df.iterrows():
        raw_article = row.get("조", "")
        link_text = (row.get("링크 텍스트") or "").strip()

        if not str(raw_article).strip() or not link_text:
            hang_col.append(None)
            ho_col.append(None)
            scope_col.append("미검출")
            matched_article_num.append("")
            continue

        key = canonicalize_article_key(raw_article)
        art = articles_by_key.get(key)

        if art is None and key.isdigit():
            cands = base_buckets.get(key, [])
            art = cands[0] if cands else None

        if art is None:
            hang_col.append(None)
            ho_col.append(None)
            scope_col.append("미검출")
            matched_article_num.append("")
            continue

        cursor_key = art["underscore"]
        cur = cursors.get(cursor_key, {"seg_idx": 0, "offset": 0})
        h, o, scope, new_cur = match_with_cursor(art, link_text, cur)
        cursors[cursor_key] = new_cur

        hang_col.append(h if scope in ("항", "호") else None)
        ho_col.append(o if scope == "호" else None)
        scope_col.append(scope)
        matched_article_num.append(art["number"])

    # 4) CSV 결과 저장
    df_out = df.copy()
    insert_pos = df_out.columns.get_loc("조") + 1
    df_out.insert(insert_pos, "항", hang_col)
    df_out.insert(insert_pos + 1, "호", ho_col)
    df_out["매칭범위"] = scope_col
    df_out["매칭조문자열"] = matched_article_num

    df_out.to_csv(OUT_CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"✅ [완료] 저장 완료 → {OUT_CSV_PATH}")
    print(f"     총 행수={len(df_out)}, 미검출={(df_out['매칭범위']=='미검출').sum()}")


# ------------ 메인 실행부 (반복문으로 변경) ------------
if __name__ == "__main__":
    # 🔽 여기에 처리할 파일 이름 목록을 추가하세요. (확장자 제외)
    file_names_to_process = [
        "해체공사표준안전작업지침",
        "추락재해방지표준안전작업지침",
        # "유해·위험방지계획서 자체심사 및 확인업체 지정대상 건설업체 고시",
        "보호구 자율안전확인 고시",
        "건설업 유해·위험방지계획서 중 지도사가 평가·확인 할 수 있는 대상 건설공사의 범위 및 지도사의 요건",
        "가설공사 표준안전 작업지침",
        "방호장치 안전인증 고시",
        # "안전인증·자율안전확인신고의 절차에 관한 고시",
        # "방호장치 자율안전기준 고시",
        # "굴착공사 표준안전 작업지침",
        # "위험기계·기구 안전인증 고시",
        # "건설업체의 산업재해예방활동 실적 평가기준",
        # "안전보건교육규정",
        # "건설공사 안전보건대장의 작성 등에 관한 고시",
        # "건설업 산업안전보건관리비 계상 및 사용기준",
        # "산업재해예방시설자금 융자금 지원사업 및 클린사업장 조성지원사업 운영규정"
    ]

    for name in file_names_to_process:
        try:
            process_law_file(name)
        except FileNotFoundError as e:
            print(f"❌ [오류] '{name}' 처리 중 파일 없음: {e}")
        except Exception as e:
            print(f"❌ [오류] '{name}' 처리 중 예외 발생: {e}")

    print("\n🎉 모든 파일 처리가 완료되었습니다.")
