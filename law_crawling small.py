import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import os


def get_sfon_number(element):
    """링크 요소의 클래스에서 sfon 번호를 정수로 추출합니다. 없으면 0을 반환합니다."""
    class_attr = element.get_attribute("class") or ""
    match = re.search(r"sfon(\d+)", class_attr)
    return int(match.group(1)) if match else 0


def scrape_law_data_with_clicks(url, output_filename):
    """
    '시행령', '시행규칙' 페이지용 크롤링 함수 (테이블 및 별표/서식/이미지 추출 기능 강화)
    """
    driver = None
    print("-" * 50)
    print(f"▶️ 작업을 시작합니다 (유형 1 - 목록형): {output_filename}")
    try:
        service = Service(ChromeDriverManager().install())
        options = webdriver.ChromeOptions()
        # 문제가 지속되면 아래 줄 맨 앞에 #을 붙여서, 브라우저가 보이는 상태로 테스트하세요.
        options.add_argument("--headless")
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--log-level=3")
        driver = webdriver.Chrome(service=service, options=options)
        driver.maximize_window()
        driver.get(url)
        wait = WebDriverWait(driver, 20)

        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.lawcon")))
            law_articles = driver.find_elements(By.CSS_SELECTOR, "div.lawcon")
        except:
            try:
                content_div = driver.find_element(By.ID, "content")
                print("✅ 'div.lawcon'이 없어 본문 전체를 수집합니다.")
                df = pd.DataFrame(
                    [
                        {
                            "조": "전체",
                            "링크 텍스트": "본문 내용",
                            "링크텍스트 클릭시 데이터": content_div.text.strip(),
                        }
                    ]
                )
                df.to_csv(output_filename, index=False, encoding="utf-8-sig")
                print(f"✅ 작업 완료! '{output_filename}' 파일로 저장되었습니다.")
                return
            except Exception as e:
                print(
                    f"❌ 오류: '{url}' 페이지에서 'div.lawcon' 또는 'content' 요소를 찾을 수 없습니다. 페이지 구조가 다를 수 있습니다. ({e})"
                )
                return

        total_articles = len(law_articles)
        final_data_list = []
        print(f"✅ 총 {total_articles}개의 '조'를 발견했습니다. 분석을 시작합니다.")
        if total_articles > 0:
            print(
                "⚠️ 이 작업은 모든 링크를 클릭하므로 시간이 매우 오래 걸릴 수 있습니다."
            )

        for i, article_div in enumerate(law_articles):
            article_num = ""
            article_logs = []
            try:
                title_element = article_div.find_element(By.CSS_SELECTOR, "p.pty1_p4")
                match = re.search(r"제(\d+(?:의\d+)?)조", title_element.text)
                article_num = match.group(1) if match else "번호 없음"
            except:
                continue

            if not article_num:
                continue

            p_tags_in_article = article_div.find_elements(By.TAG_NAME, "p")
            for p_tag in p_tags_in_article:
                links = p_tag.find_elements(By.CSS_SELECTOR, 'a.link, a[class*="sfon"]')
                if not links:
                    continue

                link_groups = []
                current_group = []
                for link in links:
                    if not current_group:
                        current_group = [link]
                        continue

                    prev_link = current_group[-1]
                    prev_sfon_num = get_sfon_number(prev_link)
                    current_sfon_num = get_sfon_number(link)
                    prev_class = prev_link.get_attribute("class") or ""
                    current_class = link.get_attribute("class") or ""
                    prev_text = prev_link.text.strip()
                    current_text = link.text.strip()
                    is_prev_link_a_law_ref = prev_text.endswith("」") or prev_text in [
                        "같은 법",
                        "같은 법 시행령",
                        "같은 법 시행규칙",
                    ]
                    is_current_link_an_article = current_text.startswith("제")
                    is_special_merge_case = (
                        is_prev_link_a_law_ref
                        and is_current_link_an_article
                        and "link" in prev_class
                        and "link" in current_class
                    )

                    break_group = False
                    if not is_special_merge_case:
                        if (
                            prev_sfon_num == 0
                            or current_sfon_num == 0
                            or current_sfon_num <= prev_sfon_num
                        ):
                            break_group = True
                        elif "sfon6" in current_class and "sfon6" not in prev_class:
                            break_group = True

                    if break_group:
                        link_groups.append(current_group)
                        current_group = [link]
                    else:
                        current_group.append(link)

                if current_group:
                    link_groups.append(current_group)

                for group in link_groups:
                    merged_text = " ".join([link.text.strip() for link in group])
                    merged_text = re.sub(r"」 제", "」제", merged_text)
                    element_to_click = group[-1]
                    new_window_text = ""
                    original_window = driver.current_window_handle
                    try:
                        driver.execute_script("arguments[0].click();", element_to_click)
                        wait.until(EC.number_of_windows_to_be(2))
                        for handle in driver.window_handles:
                            if handle != original_window:
                                driver.switch_to.window(handle)
                                break

                        try:
                            new_text_element = wait.until(
                                EC.presence_of_element_located(
                                    (By.CSS_SELECTOR, "#linkedJoContent")
                                )
                            )
                            new_window_text = new_text_element.text.strip()
                        except Exception:
                            try:
                                wait.until(
                                    EC.presence_of_element_located(
                                        (By.ID, "lsLinkTable")
                                    )
                                )
                                try:
                                    WebDriverWait(driver, 5).until(
                                        EC.presence_of_element_located(
                                            (By.CSS_SELECTOR, "#lsLinkTable p")
                                        )
                                    )
                                except Exception:
                                    pass

                                collected_texts = []

                                try:
                                    header_element = driver.find_element(
                                        By.ID, "lsLinkTableTop"
                                    )
                                    external_header_text = header_element.text.strip()
                                    if external_header_text:
                                        collected_texts.append(external_header_text)
                                except Exception:
                                    pass

                                try:
                                    internal_header_element = driver.find_element(
                                        By.CSS_SELECTOR, "#lsLinkTable > thead"
                                    )
                                    internal_header_text = (
                                        internal_header_element.text.strip()
                                    )
                                    if internal_header_text:
                                        collected_texts.append(internal_header_text)
                                except Exception:
                                    pass

                                try:
                                    table_element = driver.find_element(
                                        By.ID, "lsLinkTable"
                                    )
                                    all_p_elements = table_element.find_elements(
                                        By.XPATH, ".//p"
                                    )
                                    body_p_texts_list = []
                                    if all_p_elements:
                                        for p_elem in all_p_elements:
                                            is_in_thead = p_elem.find_elements(
                                                By.XPATH, "ancestor::thead"
                                            )
                                            if not is_in_thead:
                                                p_text = driver.execute_script(
                                                    "return arguments[0].textContent;",
                                                    p_elem,
                                                ).strip()
                                                if p_text:
                                                    body_p_texts_list.append(p_text)

                                    if body_p_texts_list:
                                        collected_texts.append(
                                            "\n".join(body_p_texts_list)
                                        )
                                    else:
                                        try:
                                            tbody_text = driver.find_element(
                                                By.CSS_SELECTOR, "#lsLinkTable > tbody"
                                            ).text.strip()
                                            if tbody_text:
                                                collected_texts.append(tbody_text)
                                        except Exception:
                                            pass
                                except Exception:
                                    pass

                                new_window_text = "\n".join(collected_texts).strip()
                            except Exception:
                                try:
                                    select_element = wait.until(
                                        EC.presence_of_element_located(
                                            (By.CSS_SELECTOR, "select#bylList")
                                        )
                                    )
                                    form_type = ""
                                    if "별지" in merged_text:
                                        form_type = "별지"
                                    elif "별표" in merged_text:
                                        form_type = "별표"
                                    elif "서식" in merged_text:
                                        form_type = "서식"

                                    num_match = re.search(
                                        r"(\d+(?:의\d+)?)", merged_text
                                    )
                                    search_text = ""
                                    if form_type and num_match:
                                        form_number = num_match.group(1)
                                        formatted_form_number = form_number.replace(
                                            "의", "의 "
                                        )
                                        search_text = (
                                            f"[{form_type} {formatted_form_number}]"
                                        )

                                    options = Select(select_element).options
                                    matched_texts = []
                                    if search_text:
                                        for option in options:
                                            option_text = option.text.strip()
                                            if option_text.startswith(search_text):
                                                matched_texts.append(option_text)

                                    if matched_texts:
                                        new_window_text = "\n".join(matched_texts)
                                    else:
                                        err_search_text = (
                                            search_text if search_text else merged_text
                                        )
                                        new_window_text = f"오류: '{err_search_text}'에 해당하는 별표/서식을 목록에서 찾을 수 없습니다."
                                except Exception:
                                    try:
                                        content_body = wait.until(
                                            EC.presence_of_element_located(
                                                (By.CSS_SELECTOR, "div.byl_con")
                                            )
                                        )
                                        new_window_text = content_body.text.strip()
                                        if not new_window_text:
                                            new_window_text = "정보: 텍스트 데이터 없음 (이미지 전용 페이지일 수 있습니다)."
                                    except Exception:
                                        new_window_text = "오류: 새 창에서 알려진 데이터 형식(#linkedJoContent, Table, #bylList, .byl_con)을 찾을 수 없습니다."

                        TEXT_LENGTH_LIMIT = 10000
                        if len(new_window_text) > TEXT_LENGTH_LIMIT:
                            new_window_text = "내용이 너무 길어 수집 제외"
                    except Exception as e:
                        new_window_text = f"오류 발생 또는 텍스트 수집 실패: {e}"
                    finally:
                        while len(driver.window_handles) > 1:
                            for handle in driver.window_handles:
                                if handle != original_window:
                                    driver.switch_to.window(handle)
                                    driver.close()
                                    break
                            time.sleep(0.1)
                        driver.switch_to.window(original_window)

                    final_data_list.append(
                        {
                            "조": article_num,
                            "링크 텍스트": merged_text,
                            "링크텍스트 클릭시 데이터": new_window_text,
                        }
                    )
                    log_data = new_window_text.replace("\n", " ").strip()
                    TRUNCATE_LIMIT = 70
                    if len(log_data) > TRUNCATE_LIMIT:
                        log_data = "..." + log_data[-TRUNCATE_LIMIT:]

                    article_logs.append(f"  ➡️  '{merged_text}'  =>  '{log_data}'")

            if total_articles > 0:
                percentage = (i + 1) / total_articles * 100
                print(
                    f"✅ [진행률: {percentage:.1f}%] 제{article_num}조 분석 완료 ({i + 1}/{total_articles})"
                )
                for log_line in article_logs:
                    print(log_line)
                print("-" * 25)

        if final_data_list:
            df = pd.DataFrame(final_data_list)
            df.to_csv(output_filename, index=False, encoding="utf-8-sig")
            print(f"✅ 작업 완료! '{output_filename}' 파일로 저장되었습니다.")
        elif total_articles == 0:
            print(
                f"⚠️ '{output_filename}' 에서 '조' 단위 데이터를 찾지 못했습니다. (작업은 정상 종료)"
            )
        else:
            print("⚠️ 수집된 데이터가 없습니다.")

    except Exception as e:
        print(f"❌ '{output_filename}' 작업 중 오류가 발생했습니다: {e}")
    finally:
        if driver:
            driver.quit()


# --- 메인 실행부 ---
if __name__ == "__main__":
    list_type_jobs = {
        # "해체공사표준안전작업지침": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186047&chrClsCd=010202&urlMode=admRulLsInfoP",
        # "추락재해방지표준안전작업지침": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186039&chrClsCd=010202&urlMode=admRulLsInfoP",
        # "철골공사표준안전작업지침": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186037&chrClsCd=010202&urlMode=admRulLsInfoP",
        "유해·위험방지계획서 자체심사 및 확인업체 지정대상 건설업체 고시": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186090&chrClsCd=010202&urlMode=admRulLsInfoP",
        # "보호구 자율안전확인 고시": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186078&chrClsCd=010202&urlMode=admRulLsInfoP",
        # "건설업 유해·위험방지계획서 중 지도사가 평가·확인 할 수 있는 대상 건설공사의 범위 및 지도사의 요건": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186089&chrClsCd=010202&urlMode=admRulLsInfoP",
        # "가설공사 표준안전 작업지침": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186031&chrClsCd=010202&urlMode=admRulLsInfoP",
        # "방호장치 안전인증 고시": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000199056&chrClsCd=010202&urlMode=admRulLsInfoP",
        # "안전인증·자율안전확인신고의 절차에 관한 고시": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000214148&chrClsCd=010202&urlMode=admRulLsInfoP",
        # "방호장치 자율안전기준 고시": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000214150&chrClsCd=010202&urlMode=admRulLsInfoP",
        # "굴착공사 표준안전 작업지침": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000226002&chrClsCd=010202&urlMode=admRulLsInfoP",
        # "위험기계·기구 안전인증 고시": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000228814&chrClsCd=010202&urlMode=admRulLsInfoP",
        "건설업체의 산업재해예방활동 실적 평가기준": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000228660&chrClsCd=010202&urlMode=admRulLsInfoP",
        "안전보건교육규정": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000239446&chrClsCd=010202&urlMode=admRulLsInfoP",
        "건설공사 안전보건대장의 작성 등에 관한 고시": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000243390&chrClsCd=010202&urlMode=admRulLsInfoP",
        "건설업 산업안전보건관리비 계상 및 사용기준": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000254546&chrClsCd=010202&urlMode=admRulLsInfoP",
        "산업재해예방시설자금 융자금 지원사업 및 클린사업장 조성지원사업 운영규정": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000255578&chrClsCd=010202&urlMode=admRulLsInfoP",
    }

    if not os.path.exists("./data"):
        os.makedirs("./data")

    for law_name, law_url in list_type_jobs.items():
        output_csv_name = f"./data/{law_name}_data.csv"
        scrape_law_data_with_clicks(law_url, output_csv_name)

    print("\n🎉 모든 작업이 완료되었습니다.")
