import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time


# ==============================================================================
# 기존 scrape_law_data_with_clicks 함수는 수정 없이 그대로 사용합니다.
# (이하 생략)
# ==============================================================================
def get_sfon_number(element):
    """링크 요소의 클래스에서 sfon 번호를 정수로 추출합니다. 없으면 0을 반환합니다."""
    class_attr = element.get_attribute("class") or ""
    match = re.search(r"sfon(\d+)", class_attr)
    return int(match.group(1)) if match else 0


def scrape_law_data_with_clicks(url, output_filename):
    """
    '시행령', '시행규칙' 페이지용 크롤링 함수 (진행률 표시 기능 추가)
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
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.lawcon")))
        law_articles = driver.find_elements(By.CSS_SELECTOR, "div.lawcon")

        # ==================================================================
        # 수정된 부분 1: 전체 '조' 개수 저장
        # ==================================================================
        total_articles = len(law_articles)
        final_data_list = []
        print(f"✅ 총 {total_articles}개의 '조'를 발견했습니다. 분석을 시작합니다.")
        print("⚠️ 이 작업은 모든 링크를 클릭하므로 시간이 매우 오래 걸릴 수 있습니다.")

        # ==================================================================
        # 수정된 부분 2: enumerate를 사용하여 인덱스(i) 추가
        # ==================================================================
        for i, article_div in enumerate(law_articles):
            article_num = ""  # article_num 초기화
            try:
                title_element = article_div.find_element(By.CSS_SELECTOR, "p.pty1_p4")
                match = re.search(r"제(\d+(?:의\d+)?)조", title_element.text)
                article_num = match.group(1) if match else ""
            except:
                continue

            # article_num이 없으면 이번 반복은 건너뜀
            if not article_num:
                continue

            # --- (기존 링크 분석 및 클릭 로직은 그대로 유지) ---
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
                        try:
                            new_text_element = wait.until(
                                EC.presence_of_element_located(
                                    (By.CSS_SELECTOR, "#linkedJoContent")
                                )
                            )
                            new_window_text = new_text_element.text.strip()
                        except Exception:
                            try:
                                select_element = wait.until(
                                    EC.presence_of_element_located(
                                        (By.CSS_SELECTOR, "select#bylList")
                                    )
                                )
                                options = select_element.find_elements(
                                    By.TAG_NAME, "option"
                                )
                                target_option_start = ""
                                if "별지" in merged_text and "서식" in merged_text:
                                    match = re.search(
                                        r"제(\d+)호(?:의(\d+))?서식", merged_text
                                    )
                                    if match:
                                        main_num, sub_num = match.groups()
                                        form_num_str = (
                                            f"{main_num}의 {sub_num}"
                                            if sub_num
                                            else main_num
                                        )
                                        target_option_start = f"[서식 {form_num_str}]"
                                elif "별표" in merged_text:
                                    match = re.search(
                                        r"(별표\s*\d+(?:의\d+)?)", merged_text
                                    )
                                    if match:
                                        bylpyo_text = match.group(1)
                                        target_option_start = f"[{bylpyo_text}]"
                                found_text = ""
                                if target_option_start:
                                    for opt in options:
                                        if opt.text.strip().startswith(
                                            target_option_start
                                        ):
                                            found_text = opt.text.strip()
                                            break
                                new_window_text = (
                                    found_text
                                    or f"'{merged_text}'에 해당하는 항목을 찾지 못했습니다."
                                )
                            except Exception:
                                new_window_text = "오류: #linkedJoContent 또는 #bylList 요소를 찾을 수 없습니다."
                        TEXT_LENGTH_LIMIT = 5000
                        if len(new_window_text) > TEXT_LENGTH_LIMIT:
                            print(
                                f"⚠️ '{merged_text}' 링크의 내용이 너무 길어 수집하지 않습니다."
                            )
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
            # --- (기존 로직 끝) ---

            # ==================================================================
            # 수정된 부분 3: 현재 '조'에 대한 작업 완료 후 진행률 출력
            # ==================================================================
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

    # 1. 기존 '시행령/규칙' 유형의 URL
    list_type_jobs = {
        # "산업안전보건법_시행규칙": 'https://www.law.go.kr/LSW/lsSc.do?section=&menuId=1&subMenuId=15&tabMenuId=81&eventGubun=060101&query=%EC%82%B0%EC%97%85%EC%95%88%EC%A0%84%EB%B3%B4%EA%B1%B4%EB%B2%95+%EC%8B%9C%ED%96%89%EA%B7%9C%EC%B9%99#undefined',
        # "산업안전보건법_시행령": "https://www.law.go.kr/LSW/lsSc.do?section=&menuId=1&subMenuId=15&tabMenuId=81&eventGubun=060101&query=%EC%82%B0%EC%97%85%EC%95%88%EC%A0%84%EB%B3%B4%EA%B1%B4%EB%B2%95+%EC%8B%9C%ED%96%89%EB%A0%B9#undefined",
        "중대재해처벌법": "https://www.law.go.kr/LSW/lsSc.do?section=&menuId=1&subMenuId=15&tabMenuId=81&eventGubun=060101&query=%EC%A4%91%EB%8C%80%EC%9E%AC%ED%95%B4%20%EC%B2%98%EB%B2%8C%20%EB%93%B1%EC%97%90%20%EA%B4%80%ED%95%9C%20%EB%B2%95%EB%A5%A0#undefined",
        "중대재해처벌법 시행령" : "https://www.law.go.kr/LSW/lsSc.do?section=&menuId=1&subMenuId=15&tabMenuId=81&eventGubun=060101&query=%EC%A4%91%EB%8C%80%EC%9E%AC%ED%95%B4%20%EC%B2%98%EB%B2%8C%20%EB%93%B1%EC%97%90%20%EA%B4%80%ED%95%9C%20%EB%B2%95%EB%A5%A0%20%EC%8B%9C%ED%96%89%EB%A0%B9#undefined",
        "안전보건규칙": "https://www.law.go.kr/LSW/lsSc.do?section=&menuId=1&subMenuId=15&tabMenuId=81&eventGubun=060101&query=%EC%82%B0%EC%97%85%EC%95%88%EC%A0%84%EB%B3%B4%EA%B1%B4%EA%B8%B0%EC%A4%80%EC%97%90%20%EA%B4%80%ED%95%9C%20%EA%B7%9C%EC%B9%99#undefined"
    }

    # 각 유형에 맞는 함수를 호출하여 크롤링 수행
    for law_name, law_url in list_type_jobs.items():
        output_csv_name = f"./data/{law_name}_data.csv"
        scrape_law_data_with_clicks(law_url, output_csv_name)  # 기존 함수 호출

    print("\n🎉 모든 작업이 완료되었습니다.")
