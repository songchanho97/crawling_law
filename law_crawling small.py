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
            # 'div.lawcon'이 없는 페이지는 링크가 없는 단순 고시일 수 있으므로, 다른 방법으로 텍스트를 수집
            try:
                content_div = driver.find_element(By.ID, "content")
                print("✅ 'div.lawcon'이 없어 본문 전체를 수집합니다.")
                # 이 경우, 링크 분석이 무의미하므로 데이터를 생성하고 함수를 종료합니다.
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
                    # =================== ✨ 오타 수정된 부분 ✨ ===================
                    current_text = link.text.strip()  # 'current_link'를 'link'로 수정
                    # ==========================================================

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
                                thead = driver.find_element(By.ID, "lsLinkTableTop")
                                tbody = driver.find_element(By.ID, "lsLinkTable")
                                new_window_text = (
                                    f"{thead.text.strip()}\n{tbody.text.strip()}"
                                )
                            except Exception:
                                try:
                                    wait.until(
                                        EC.presence_of_element_located(
                                            (By.CSS_SELECTOR, "select#bylList")
                                        )
                                    )
                                    new_window_text = (
                                        "별표/서식 데이터 (처리 로직 생략)"
                                    )
                                except Exception:
                                    new_window_text = "오류: 새 창에서 알려진 데이터 형식(#linkedJoContent, Table, #bylList)을 찾을 수 없습니다."

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
            if total_articles > 0:
                percentage = (i + 1) / total_articles * 100
                print(
                    f"⏳ [진행률: {percentage:.1f}%] 제{article_num}조 분석 완료 ({i + 1}/{total_articles})"
                )

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
        "건설업 유해·위험방지계획서 중 지도사가 평가·확인 할 수 있는 대상 건설공사의 범위 및 지도사의 요건": "https://www.law.go.kr/LSW/admRulInfoP.do?admRulSeq=2100000186089&chrClsCd=010202&urlMode=admRulLsInfoP"
        # (이하 URL 목록은 동일하므로 생략)
    }

    for law_name, law_url in list_type_jobs.items():
        output_csv_name = f"./data/{law_name}_data.csv"
        scrape_law_data_with_clicks(law_url, output_csv_name)

    print("\n🎉 모든 작업이 완료되었습니다.")
