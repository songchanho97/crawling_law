# -*- coding: utf-8 -*-
import pandas as pd
import re

file_name = "./data/산업안전보건법_시행령"
current_name = "산업안전보건법 시행령"

# ========================
# 경로/시트 설정
# ========================
IN_XLSX = f"{file_name}_항_호.xlsx"  # 입력 엑셀 파일
SHEET_NAME = None  # None이면 첫 시트 사용, 특정 시트명 지정 가능
OUT_XLSX = f"{file_name}_labeled.xlsx"  # 결과 엑셀 파일


# ========================
# 유틸
# ========================
def read_excel_first_or_named(path: str, sheet_name=None) -> tuple[pd.DataFrame, str]:
    """엑셀에서 sheet_name 있으면 해당 시트, 없으면 첫 시트 읽기."""
    xls = pd.ExcelFile(path)
    if sheet_name and sheet_name in xls.sheet_names:
        name = sheet_name
    else:
        name = xls.sheet_names[0]
    print(f"[INFO] 시트 선택: {name}")
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


def build_id(row) -> str:
    """산업안전보건법-조(항)[호] 생성 (항/호 비어있으면 해당 부분 생략)."""
    j = "" if pd.isna(row.get("조")) else str(row.get("조")).strip()
    a = clean_num_str(row.get("항"))
    h = clean_num_str(row.get("호"))
    if j == "":
        return ""
    _id = f"{current_name}-{j}"
    if a != "":
        _id += f"({a})"
    if h != "":
        _id += f"[{h}]"
    return _id


# ========================
# 실행
# ========================
# 1) 엑셀 읽기
df, chosen_sheet = read_excel_first_or_named(IN_XLSX, SHEET_NAME)

# 2) 기본 컬럼 확인/보강
need_cols = {"조", "링크 텍스트"}
missing = need_cols - set(df.columns)
if missing:
    raise ValueError(f"엑셀에 필요한 컬럼이 없습니다: {missing}")

# '항','호'가 없으면 생성(전부 비어있는 상태)
for col in ["항", "호"]:
    if col not in df.columns:
        df[col] = None

# 원본 보존(비교/검증용)
if "링크 텍스트(원본)" not in df.columns:
    df["링크 텍스트(원본)"] = df["링크 텍스트"]

# 3) 그룹핑 키 만들기 (빈값 정규화: NaN -> '')
df["_조"] = df["조"].fillna("").astype(str).str.strip()
df["_항"] = df["항"].fillna("").astype(str).str.strip()
df["_호"] = df["호"].fillna("").astype(str).str.strip()

# 4) 링크 텍스트의 기본형(뒤의 (n) 제거) 컬럼 생성
df["_링크텍스트_base"] = (
    df["링크 텍스트"].fillna("").astype(str).map(strip_trailing_index)
)

# 5) (조,항,호) 그룹 내에서 같은 링크 텍스트(base)가 반복되면 (1),(2)… 부여
#    단일 건은 suffix 없음
group_cols = ["_조", "_항", "_호", "_링크텍스트_base"]
gb = df[group_cols].groupby(group_cols, dropna=False)

# 그룹 내 순번(1부터)
order_in_group = gb.cumcount() + 1
# 그룹 크기
group_sizes = gb["_링크텍스트_base"].transform("size")
dup_mask = group_sizes > 1

# 새 링크 텍스트 만들기
df["링크 텍스트"] = df["_링크텍스트_base"]  # 기본형으로 통일
df.loc[dup_mask, "링크 텍스트"] = (
    df.loc[dup_mask, "_링크텍스트_base"].astype(str)
    + "("
    + order_in_group[dup_mask].astype(str)
    + ")"
)

# 6) id 생성: 산업안전보건법-조(항)[호]
df["id"] = df.apply(build_id, axis=1)

# 7) 보조 컬럼 제거
df = df.drop(columns=["_조", "_항", "_호", "_링크텍스트_base"])

# 8) 엑셀로 저장(한 시트)
with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
    df.to_excel(writer, index=False, sheet_name=chosen_sheet)

print("[OK] 엑셀 저장 완료 →", OUT_XLSX)
