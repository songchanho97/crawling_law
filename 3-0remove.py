# -*- coding: utf-8 -*-
import pandas as pd
import re
import os

# ========================
# 처리할 파일 목록 설정
# ========================
# 여기에 처리하고 싶은 파일 정보를 추가하세요.
# "file_base": "./data/" 폴더에 있는 엑셀 파일의 기본 이름 (확장자 제외, "_항_호" 제외)
# "display_name": ID 생성 시 사용될 공식 법령 이름

# in_xlsx = f"{file_base}_항_호.xlsx"
# out_xlsx = f"{file_base}_labeled.xlsx"
# --------------------------------------------------------------------------

LAWS_TO_PROCESS = [
    {
        "file_base": "./data/고시및예규/해체공사표준안전작업지침",
        "display_name": "해체공사표준안전작업지침",
    },
    {
        "file_base": "./data/고시및예규/추락재해방지표준안전작업지침",
        "display_name": "추락재해방지표준안전작업지침",
    },
    {
        "file_base": "./data/고시및예규/유해·위험방지계획서 자체심사 및 확인업체 지정대상 건설업체 고시",
        "display_name": "유해·위험방지계획서 자체심사 및 확인업체 지정대상 건설업체 고시",
    },
    {
        "file_base": "./data/고시및예규/보호구 자율안전확인 고시",
        "display_name": "보호구 자율안전확인 고시",
    },
    {
        "file_base": "./data/고시및예규/건설업 유해·위험방지계획서 중 지도사가 평가·확인 할 수 있는 대상 건설공사의 범위 및 지도사의 요건",
        "display_name": "건설업 유해·위험방지계획서 중 지도사가 평가·확인 할 수 있는 대상 건설공사의 범위 및 지도사의 요건",
    },
    {
        "file_base": "./data/고시및예규/가설공사 표준안전 작업지침",
        "display_name": "가설공사 표준안전 작업지침",
    },
    {
        "file_base": "./data/고시및예규/방호장치 안전인증 고시",
        "display_name": "방호장치 안전인증 고시",
    },
    # {
    #     "file_base": "./data/안전인증·자율안전확인신고의 절차에 관한 고시",
    #     "display_name": "안전인증·자율안전확인신고의 절차에 관한 고시"
    # },
    # {
    #     "file_base": "./data/방호장치 자율안전기준 고시",
    #     "display_name": "방호장치 자율안전기준 고시"
    # },
    # {
    #     "file_base": "./data/굴착공사 표준안전 작업지침",
    #     "display_name": "굴착공사 표준안전 작업지침"
    # },
    # {
    #     "file_base": "./data/위험기계·기구 안전인증 고시",
    #     "display_name": "위험기계·기구 안전인증 고시"
    # },
    # {
    #     "file_base": "./data/건설업체의 산업재해예방활동 실적 평가기준",
    #     "display_name": "건설업체의 산업재해예방활동 실적 평가기준"
    # },
    # {
    #     "file_base": "./data/안전보건교육규정",
    #     "display_name": "안전보건교육규정"
    # },
    # {
    #     "file_base": "./data/건설공사 안전보건대장의 작성 등에 관한 고시",
    #     "display_name": "건설공사 안전보건대장의 작성 등에 관한 고시"
    # },
    # {
    #     "file_base": "./data/건설업 산업안전보건관리비 계상 및 사용기준",
    #     "display_name": "건설업 산업안전보건관리비 계상 및 사용기준"
    # },
    # {
    #     "file_base": "./data/산업재해예방시설자금 융자금 지원사업 및 클린사업장 조성지원사업 운영규정",
    #     "display_name": "산업재해예방시설자금 융자금 지원사업 및 클린사업장 조성지원사업 운영규정"
    # }
]


# ========================
# 유틸리티 함수 (기존과 동일)
# ========================
def read_excel_first_or_named(path: str, sheet_name=None) -> tuple[pd.DataFrame, str]:
    """엑셀에서 sheet_name 있으면 해당 시트, 없으면 첫 시트 읽기."""
    xls = pd.ExcelFile(path)
    if sheet_name and sheet_name in xls.sheet_names:
        name = sheet_name
    else:
        name = xls.sheet_names[0]
    print(f"  [INFO] 시트 선택: {name}")
    df = pd.read_excel(xls, sheet_name=name, dtype=str)
    return df, name


def clean_num_str(x) -> str:
    """'2', '2.0', ' 3 ' -> '2','2','3' / NaN->''"""
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if s == "":
        return ""
    try:
        v = float(s)
        if v.is_integer():
            return str(int(v))
        else:
            return s
    except Exception:
        return s


def strip_trailing_index(s: str) -> str:
    """문자열 끝의 '(숫자)' 패턴을 제거 (예: '근로기준법(2)' -> '근로기준법')."""
    if s is None:
        return ""
    return re.sub(r"\(\d+\)$", "", str(s)).strip()


def build_id(row, law_name: str) -> str:
    """산업안전보건법-조(항)[호] 생성 (항/호 비어있으면 해당 부분 생략)."""
    j = "" if pd.isna(row.get("조")) else str(row.get("조")).strip()
    a = clean_num_str(row.get("항"))
    h = clean_num_str(row.get("호"))
    if j == "":
        return ""
    _id = f"{law_name}-{j}"
    if a != "":
        _id += f"({a})"
    if h != "":
        _id += f"[{h}]"
    return _id


# ========================
# 핵심 로직 함수
# ========================
def process_file(file_base: str, display_name: str, sheet_name=None):
    """단일 엑셀 파일을 읽어 처리하고 저장하는 함수."""

    in_xlsx = f"{file_base}_항_호.xlsx"
    out_xlsx = f"{file_base}_labeled.xlsx"

    # 0) 입력 파일 존재 확인
    if not os.path.exists(in_xlsx):
        print(f"[ERROR] 입력 파일을 찾을 수 없습니다: {in_xlsx}\n")
        return

    print(f"▶️  '{display_name}' 파일 처리 시작...")

    # 1) 엑셀 읽기
    df, chosen_sheet = read_excel_first_or_named(in_xlsx, sheet_name)

    # 2) 기본 컬럼 확인/보강
    need_cols = {"조", "링크 텍스트"}
    missing = need_cols - set(df.columns)
    if missing:
        raise ValueError(f"엑셀에 필요한 컬럼이 없습니다: {missing}")

    for col in ["항", "호"]:
        if col not in df.columns:
            df[col] = None

    if "링크 텍스트(원본)" not in df.columns:
        df["링크 텍스트(원본)"] = df["링크 텍스트"]

    # 3) 그룹핑 키 만들기
    df["_조"] = df["조"].fillna("").astype(str).str.strip()
    df["_항"] = df["항"].fillna("").astype(str).str.strip()
    df["_호"] = df["호"].fillna("").astype(str).str.strip()

    # 4) 링크 텍스트의 기본형 컬럼 생성
    df["_링크텍스트_base"] = (
        df["링크 텍스트"].fillna("").astype(str).map(strip_trailing_index)
    )

    # 5) (조,항,호) 그룹 내 중복 링크 텍스트에 번호 부여
    group_cols = ["_조", "_항", "_호", "_링크텍스트_base"]
    gb = df[group_cols].groupby(group_cols, dropna=False)
    order_in_group = gb.cumcount() + 1
    group_sizes = gb["_링크텍스트_base"].transform("size")
    dup_mask = group_sizes > 1

    df["링크 텍스트"] = df["_링크텍스트_base"]
    df.loc[dup_mask, "링크 텍스트"] = (
        df.loc[dup_mask, "_링크텍스트_base"].astype(str)
        + "("
        + order_in_group[dup_mask].astype(str)
        + ")"
    )

    # 6) id 생성
    df["id"] = df.apply(lambda row: build_id(row, display_name), axis=1)

    # 7) 보조 컬럼 제거
    df = df.drop(columns=["_조", "_항", "_호", "_링크텍스트_base"])

    # 8) 엑셀로 저장
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=chosen_sheet)

    print(f"✅ 엑셀 저장 완료 → {out_xlsx}\n")


# ========================
# 실행
# ========================
if __name__ == "__main__":
    print("===== 작업 시작 =====")
    for law_info in LAWS_TO_PROCESS:
        try:
            process_file(
                file_base=law_info["file_base"], display_name=law_info["display_name"]
            )
        except Exception as e:
            print(f"🚨 처리 중 오류 발생: {law_info['display_name']}")
            print(f"  오류 내용: {e}\n")
    print("===== 모든 작업 완료 =====")
