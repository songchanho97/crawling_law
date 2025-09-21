import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time


def get_sfon_number(element):
    """링크 요소의 클래스에서 sfon 번호를 정수로 추출합니다. 없으면 0을 반환합니다."""
    class_attr = element.get_attribute("class") or ""
    match = re.search(r"sfon(\d+)", class_attr)
    return int(match.group(1)) if match else 0


def scrape_law_data_with_clicks(url, output_filename):
    """
    '시행령', '시행규칙' 페이지용 크롤링 함수 (테이블 추출 기능 추가)
    """
    driver = None
    print("-" * 50)
    print(f"▶️ 작업을 시작합니다 (유형 1 - 목록형): {output_filename}")
    try:
        service = Service(ChromeDriverManager().install())
        options = webdriver.ChromeOptions()
        # options.add_argument("--headless") # 디버깅을 위해 잠시 주석 처리
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--log-level=3")
        driver = webdriver.Chrome(service=service, options=options)
        driver.maximize_window()
        driver.get(url)
        wait = WebDriverWait(driver, 20)

        # 'div.lawcon'이 없는 페이지일 경우를 대비한 예외 처리
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.lawcon")))
            law_articles = driver.find_elements(By.CSS_SELECTOR, "div.lawcon")
        except:
            print(
                f"❌ 오류: '{url}' 페이지에서 'div.lawcon' 요소를 찾을 수 없습니다. 페이지 구조가 다를 수 있습니다."
            )
            return

        total_articles = len(law_articles)
        final_data_list = []
        print(f"✅ 총 {total_articles}개의 '조'를 발견했습니다. 분석을 시작합니다.")
        print("⚠️ 이 작업은 모든 링크를 클릭하므로 시간이 매우 오래 걸릴 수 있습니다.")

        for i, article_div in enumerate(law_articles):
            article_num = ""
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

                # (링크 그룹화 로직은 기존과 동일)
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
                    break_group = False
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

                        # ==========================================================
                        # ✨ 여기가 핵심 수정 부분 (테이블 추출 로직 추가) ✨
                        # ==========================================================
                        try:
                            # 1순위: 법령 조문(#linkedJoContent) 시도
                            new_text_element = wait.until(
                                EC.presence_of_element_located(
                                    (By.CSS_SELECTOR, "#linkedJoContent")
                                )
                            )
                            new_window_text = new_text_element.text.strip()
                        except Exception:
                            try:
                                # 2순위: 테이블 데이터 (#lsLinkTableTop, #lsLinkTable) 시도
                                wait.until(
                                    EC.presence_of_element_located(
                                        (By.ID, "lsLinkTable")
                                    )
                                )
                                thead = driver.find_element(By.ID, "lsLinkTableTop")
                                tbody = driver.find_element(By.ID, "lsLinkTable")
                                header_text = thead.text.strip()
                                body_text = tbody.text.strip()
                                new_window_text = f"{header_text}\n{body_text}"
                            except Exception:
                                try:
                                    # 3순위: 별표/서식 목록(#bylList) 시도
                                    select_element = wait.until(
                                        EC.presence_of_element_located(
                                            (By.CSS_SELECTOR, "select#bylList")
                                        )
                                    )
                                    options = select_element.find_elements(
                                        By.TAG_NAME, "option"
                                    )
                                    # (이하 별표/서식 처리 로직은 기존과 동일)
                                    # ...
                                    new_window_text = (
                                        "별표/서식 데이터 (처리 로직 생략)"
                                    )
                                except Exception:
                                    new_window_text = "오류: 새 창에서 알려진 데이터 형식(#linkedJoContent, Table, #bylList)을 찾을 수 없습니다."

                        # 텍스트 길이 제한
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

            percentage = (i + 1) / total_articles * 100
            print(
                f"⏳ [진행률: {percentage:.1f}%] 제{article_num}조 분석 완료 ({i + 1}/{total_articles})"
            )

        if final_data_list:
            df = pd.DataFrame(final_data_list)
            df.to_csv(output_filename, index=False, encoding="utf-8-sig")
            print(f"✅ 작업 완료! '{output_filename}' 파일로 저장되었습니다.")
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
        "해체공사표준안전작업지침": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186047&chrClsCd=010202&urlMode=admRulLsInfoP",
        "추락재해방지표준안전작업지침":"https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186039&chrClsCd=010202&urlMode=admRulLsInfoP",
        "철골공사표준안전작업지침":"https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186037&chrClsCd=010202&urlMode=admRulLsInfoP",
        # "유해·위험방지계획서 자체심사 및 확인업체 지정대상 건설업체 고시":"https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186090&chrClsCd=010202&urlMode=admRulLsInfoP",
        # "보호구 자율안전확인 고시":"https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186078&chrClsCd=010202&urlMode=admRulLsInfoP",
        # "건설업 유해·위험방지계획서 중 지도사가 평가·확인 할 수 있는 대상 건설공사의 범위 및 지도사의 요건":"https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186089&chrClsCd=010202&urlMode=admRulLsInfoP",
        #  "가설공사 표준안전 작업지침":"https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186031&chrClsCd=010202&urlMode=admRulLsInfoP",
        #  "방호장치 안전인증 고시":"https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000199056&chrClsCd=010202&urlMode=admRulLsInfoP",
        #  "안전인증·자율안전확인신고의 절차에 관한 고시":"https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000214148&chrClsCd=010202&urlMode=admRulLsInfoP",
        #  "방호장치 자율안전기준 고시":"https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000214150&chrClsCd=010202&urlMode=admRulLsInfoP",
        #  "굴착공사 표준안전 작업지침":"https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000226002&chrClsCd=010202&urlMode=admRulLsInfoP",
        #  "위험기계·기구 안전인증 고시":"https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000228814&chrClsCd=010202&urlMode=admRulLsInfoP",
        #  "건설업체의 산업재해예방활동 실적 평가기준":"https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000228660&chrClsCd=010202&urlMode=admRulLsInfoP",
        #  "안전보건교육규정":"https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000239446&chrClsCd=010202&urlMode=admRulLsInfoP",
        #  "건설공사 안전보건대장의 작성 등에 관한 고시": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000243390&chrClsCd=010202&urlMode=admRulLsInfoP",
        #  "건설업 산업안전보건관리비 계상 및 사용기준": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000254546&chrClsCd=010202&urlMode=admRulLsInfoP",
        #  "산업재해예방시설자금 융자금 지원사업 및 클린사업장 조성지원사업 운영규정": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000255578&chrClsCd=010202&urlMode=admRulLsInfoP"
    }

    for law_name, law_url in list_type_jobs.items():
        output_csv_name = f"./data/{law_name}_data.csv"
        scrape_law_data_with_clicks(law_url, output_csv_name)

    print("\n🎉 모든 작업이 완료되었습니다.")
