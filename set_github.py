# เพื่อเปิด Certificate
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import pandas as pd
import requests
import os
import shutil
import time
import threading
import queue
from io import StringIO
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml import html as lxml_html
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ==========================================
# CONFIGURATION
# ==========================================
MAX_WORKERS = 2  # จำนวนหน้าต่าง Chrome ที่จะเปิดพร้อมกัน (ปรับตามความแรงเครื่อง)
HEADLESS_MODE = True # แนะนำให้เป็น True เพื่อความเร็วและไม่เกะกะ
WAIT_TIMEOUT = 10 # วินาทีสูงสุดที่จะรอ element

# Path Configuration
LOCAL_PATH = "./" 
LOCAL_PATH_DATA = "./data_factsheet/"

# XPATH Mappings (คงเดิม)
XPATH_MAPPINGS = {
    "Nature Business": '//div[@class="nature-business"]//div[@class="row col-12"]',
    "Card Minimal Left": '//div[@class="price-info-wrapper"]//div[@class="card-minimal"]//div//div[@class="price-left-col"]',
    "Card Minimal Right": '//div[@class="price-info-wrapper"]//div[@class="card-minimal"]//div//div[@class="price-right-col"]',
    "Company Info": '//div[@class="company-info d-flex flex-column me-auto"]',
    "Basic Information": '//div[@class="row issuer-info border-info"]',
    # "Annual Report": '//div[@class="row issuer-info border-info"]//div[label[contains(text(), "รายงานประจำปี")]]//a',
    "Annual Report": '//div[@class="row issuer-info border-info"]//div[@class="basic-content align-self-baseline"]//div[label[contains(text(), "รายงานประจำปี")]]//div//a',
    "Basic Content 1": '//div[@class="col-6 basic-content align-self-baseline"]',
    "Basic Content 2": '//div[@class="col-6 mb-2 basic-content align-self-baseline"]',
    "Dividend Policy": '//div[@class="d-flex flex-wrap text-dark text-policy fs-12px pe-3 ps-3 py-1"]',
    "Auditor": '//div[@class="auditor col-basic-info col-12 col-lg-6 mb-3 mb-lg-0 px-4"]',
    "News Wrapper": '//div[@class="row news-wrapper"]',
    "Name": '//h1[@class="company-name title-font-family fs-24px"]',
    "Card Holder": '//div[@class="card-holder"]//div[@class="card-minimal"]//div[@class="card-minimal-body mb-0 p-0"]',
    "Card Board": '//div[@class="card-board"]//div[@class="card-minimal"]//div[@class="card-minimal-body mb-0 p-0"]',
    "Adjust Price": '//div[@class="card-history"]//div[@class="card-minimal"]//div[@class="card-minimal-body mb-0 p-0"]',
    "Stat": '//div[@class="card-stat"]//div[@class="card-minimal"]//div[@class="card-minimal-body mb-0 p-0"]',
    "Return": '//div[@class="card-yield"]//div[@class="card-minimal"]//div[@class="card-minimal-body mb-0 p-0"]',
    "Dividend": '//div[@class="card-dividend"]//div[@class="card-minimal mb-3"]//div[@class="card-minimal-body mb-0 p-0"]',
    "FinanStateSubmit": '//div[@class="FinanStateSubmit col-basic-info col-12 col-lg-6 mb-3 mb-lg-0"]//div[@class="card-minimal"]//div[@class="card-minimal-body mb-0 p-0"]',
    "BS": '(//div[@class="factsheet-financial mt-2"]//div[@class="table-custom-field-main mb-1 table-lg-noresponsive"])[1]',
    "PL": '(//div[@class="factsheet-financial mt-2"]//div[@class="table-custom-field-main mb-1 table-lg-noresponsive"])[2]',
    "CF": '(//div[@class="factsheet-financial mt-2"]//div[@class="table-custom-field-main mb-1 table-lg-noresponsive"])[3]',
    "Ratio": '//div[@class="factsheet-financial mt-2"]//div[@class="table-custom-field-main mb-5 table-lg-noresponsive"]',
    "Change": '//div[@class="factsheet-financial mt-2"]//div[@class="row row-mini-table"]//div[@class="col-12 col-lg-6 rate-change"]',
    "Cycle": '//div[@class="factsheet-financial mt-2"]//div[@class="row row-mini-table"]//div[@class="col-12 col-lg-6 cash-cycle"]',
    "Cap": '(//div[@class="factsheet-financial mt-2"]//div[@class="row mb-3"]//div[@class="cell-50 col-12 col-lg-6 mb-lg-0"]//div[@class="card-minimal mb-4"])[1]',
    "Sign": '(//div[@class="factsheet-financial mt-2"]//div[@class="row mb-3"]//div[@class="cell-50 col-12 col-lg-6 mb-lg-0"]//div[@class="card-minimal mb-4"])[2]',
    "Oth": '//div[@class="factsheet-financial mt-2"]//div[@class="row mb-3"]//div[@class="cell-50 col-12 col-lg-6"]//div[@class="card-minimal mb-4"]',
    "Alert": '//div[@class="factsheet-financial mt-2"]//div[@class="row mb-3"]//div[@class="cell-50 col-12 col-lg-6"]//div[@class="card-minimal"]'
}

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def create_driver():
    options = webdriver.ChromeOptions()
    if HEADLESS_MODE:
        options.add_argument("--headless") # สำคัญมากสำหรับการรันบน GitHub
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    
    # ใช้ ChromeDriverManager แทนการระบุ Path ในเครื่องตัวเอง
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def setup_environment():
    """เตรียม Folder และไฟล์ตั้งต้น"""
    print("🔧 กำลังเตรียม Environment...")
    if os.path.exists(LOCAL_PATH_DATA):
        shutil.rmtree(LOCAL_PATH_DATA)
    os.makedirs(LOCAL_PATH_DATA, exist_ok=True)
    print(f"✅ สร้างโฟลเดอร์ {LOCAL_PATH_DATA} แล้ว")

    # Download & Prepare Master File
    xls_path = os.path.join(LOCAL_PATH, 'listedCompanies_th_TH.xls')
    csv_path = os.path.join(LOCAL_PATH, 'listedCompanies_th_TH.csv')
    
    # Clean old files
    for p in [xls_path, csv_path]:
        if os.path.exists(p):
            os.remove(p)

    # Download
    url = "https://www.set.or.th/dat/eod/listedcompany/static/listedCompanies_th_TH.xls"
    print(f"📥 กำลังดาวน์โหลด {url}...")
    # resp = requests.get(url) # <--- (รันแล้วติด Certificate ที่ office) จุดที่ Error

    # เพิ่ม verify=False เพื่อข้ามการตรวจ SSL
    resp = requests.get(url, verify=False)
    with open(xls_path, "wb") as f:
        f.write(resp.content)
    
    # Convert to CSV
    print("🔄 กำลังแปลงไฟล์เป็น CSV...")
    df_list = pd.read_html(xls_path, encoding="ISO-8859-11")
    df = df_list[0]
    df = df.iloc[1:].reset_index(drop=True) # ลบแถวแรก
    df.columns = df.iloc[0] # ตั้ง header
    df = df[1:].reset_index(drop=True) # ลบแถว header ซ้ำ
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print("✅ เตรียมไฟล์ตั้งต้นเสร็จสิ้น")
    return df

def parse_page_content(html_content, page_url):
    """แกะข้อมูลจาก HTML string โดยใช้ lxml (เร็วกว่า Selenium find_element มาก)"""
    tree = lxml_html.fromstring(html_content)
    extracted_data = []

    for label, xpath in XPATH_MAPPINGS.items():
        try:
            elements = tree.xpath(xpath)
            for el in elements:
                # แปลง element กลับเป็น html string เพื่อเช็ค table
                el_html = lxml_html.tostring(el, encoding='unicode')
                
                # --- ส่วนที่เพิ่มของการดึง annual report ---
                if label == "Annual Report":
                    # ดึงค่าจาก attribute 'href'
                    link = el.get('href')
                    if link:
                        extracted_data.append([label, page_url, link])
                    continue # ทำตัวถัดไปเลย ไม่ต้องไปเช็ค table ต่อ

                if "<table" in el_html.lower():
                    try:
                        # ใช้ pandas read_html กับ string
                        tables = pd.read_html(StringIO(el_html))
                        for df in tables:
                            if isinstance(df.columns, pd.MultiIndex):
                                df.columns = [' | '.join([str(c) for c in col if c]) for col in df.columns]
                            else:
                                df.columns = df.columns.astype(str)

                            header_row = [label, page_url] + df.columns.tolist()
                            extracted_data.append(header_row)

                            for row in df.itertuples(index=False, name=None):
                                extracted_data.append([label, page_url] + list(row))
                    except Exception:
                        pass # Table parse error
                else:
                    # Text content
                    text = el.text_content().strip()
                    if text:
                        extracted_data.append([label, page_url, text])
        except Exception:
            pass # XPath error

    return extracted_data

def worker_thread(url_queue, result_list):
    """Worker thread ที่จะเปิด Browser ค้างไว้แล้วรับงานจาก Queue (แก้ไขเพิ่ม Delay + Smart Wait Body)"""
    driver = None
    try:
        driver = create_driver()
        while True:
            try:
                url = url_queue.get(timeout=3) # รองาน 3 วิ ถ้าไม่มีคือจบ
            except queue.Empty:
                break
            
            try:
                driver.get(url)
                
                # 1. Smart Wait 1: รอให้โครงสร้างหลักโผล่มาก่อน (Company Info)
                WebDriverWait(driver, WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "company-info"))
                )

                # ==================================================
                # 🛠️ ส่วนที่แก้ไข: Smart Wait 2: รอให้ Body/DOM พร้อมใช้งาน (แก้ Error scrollHeight)
                # ==================================================
                
                # รอก่อนสั่ง Scroll เพื่อให้มั่นใจว่า document.body ถูกสร้างแล้ว
                WebDriverWait(driver, WAIT_TIMEOUT).until(
                    lambda d: d.execute_script('return document.body != null && document.body.scrollHeight > 0;')
                )
                
                # สั่ง Scroll ลงมากลาง ๆ เพื่อกระตุ้น Lazy Load (ถ้ามี)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                time.sleep(1) # รอจังหวะ scroll
                
                # รอให้ตัวเลขงบการเงินวิ่งมาให้ครบ (สำคัญมาก, เพิ่มจาก 3 เป็น 5 เพื่อความชัวร์, แก้ Read timed out)
                time.sleep(5) 
                
                # ==================================================

                # ดึง Source ทีเดียว
                html_source = driver.page_source
                
                # ส่งไปแกะข้อมูล (CPU bound)
                data = parse_page_content(html_source, url)
                
                # Thread-safe append
                result_list.extend(data)
                
                # ตรวจสอบความสำเร็จจากข้อมูลที่ได้
                if data:
                    print(f"  ✓ Scraped: {url.split('/')[-2]}")
                else:
                    print(f"  ⚠️ Scraped (No Data): {url.split('/')[-2]}") # พบแล้วแต่ไม่มีข้อมูลเลย
                
            except TimeoutException as e:
                print(f"  ❌ Error {url}: Timeout (Page did not load key elements)")
            except WebDriverException as e:
                # Catch JavaScript Error & Timeout/Connection Error
                print(f"  ❌ Error {url}: WebDriver Error ({e.msg.split('\\n')[0]})")
            except Exception as e:
                # Catch All Other Errors
                print(f"  ❌ Error {url}: Unexpected Error ({type(e).__name__}) - {e}")
            finally:
                url_queue.task_done()
                
    except Exception as e:
        print(f"🔥 Worker Initialization Error: {e}")
    finally:
        if driver:
            driver.quit()

def process_sector(sector_name, stock_list):
    """จัดการการดึงข้อมูลของ 1 Sector"""
    print(f"\n🚀 เริ่มประมวลผลกลุ่ม: {sector_name} ({len(stock_list)} บริษัท)")
    
    # สร้าง URL List
    urls = [f"https://www.set.or.th/th/market/product/stock/quote/{stock}/factsheet" for stock in stock_list]
    
    # ใส่ Queue
    q = queue.Queue()
    for u in urls:
        q.put(u)
        
    # Shared Result List
    sector_data = []
    
    # Start Workers
    threads = []
    # จำกัด Worker ไม่เกิน URL ที่มี
    num_workers = min(MAX_WORKERS, len(urls))
    
    for _ in range(num_workers):
        t = threading.Thread(target=worker_thread, args=(q, sector_data))
        t.start()
        threads.append(t)
        
    # Wait for all threads
    for t in threads:
        t.join()
        
    # Save CSV
    if sector_data:
        max_cols = max(len(r) for r in sector_data)
        cols = ["TableName", "URL"] + [f"Column{i+1}" for i in range(max_cols - 2)]
        df_out = pd.DataFrame(sector_data, columns=cols)
        
        filename = f"{sector_name.lower()}_company.csv"
        # Mapping ชื่อไฟล์ให้ตรงกับของเดิม
        name_map = {
            "AGRO": "agro_company.csv",
            "CONSUMP": "consump_company.csv",
            "FINCIAL": "fincial_company.csv",
            "INDUS": "indus_company.csv",
            "PROPCON": "propcon_company.csv",
            "RESOURC": "resourc_company.csv",
            "SERVICE": "service_company.csv",
            "TECH": "tech_company.csv",
            "-": "na_company.csv"
        }
        if sector_name in name_map:
            filename = name_map[sector_name]
            
        out_path = os.path.join(LOCAL_PATH_DATA, filename)
        df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"💾 บันทึก {filename} เรียบร้อย ({len(df_out)} rows)")
    else:
        print(f"⚠️ ไม่มีข้อมูลสำหรับ {sector_name}")

# ==========================================
# MAIN EXECUTION (STEP 1: SCRAPE)
# ==========================================

start_time = time.time()

# 1. Setup
df_master = setup_environment()

# 2. Define Sectors to Process
# (ชื่อกลุ่มใน CSV, ชื่อย่อที่จะใช้แสดงผล)
sectors = [
    ('เกษตรและอุตสาหกรรมอาหาร', 'AGRO'),
    ('สินค้าอุปโภคบริโภค', 'CONSUMP'),
    ('ธุรกิจการเงิน', 'FINCIAL'),
    ('สินค้าอุตสาหกรรม', 'INDUS'),
    ('อสังหาริมทรัพย์และก่อสร้าง', 'PROPCON'),
    ('ทรัพยากร', 'RESOURC'),
    ('บริการ', 'SERVICE'),
    ('เทคโนโลยี', 'TECH'),
    ('-', '-') 
]

# 3. Loop Process
for sector_th, sector_en in sectors:
    # Filter Stock List
    stock_list = df_master.loc[df_master['กลุ่มอุตสาหกรรม'] == sector_th, 'หลักทรัพย์'].tolist()
    
    if not stock_list:
        print(f"ข้าม {sector_en} (ไม่พบหุ้น)")
        continue
        
    process_sector(sector_en, stock_list)
    
total_time = time.time() - start_time
print(f"\n✨ เสร็จสิ้นการดึงข้อมูลรอบแรกในเวลา: {total_time/60:.2f} นาที ✨")

# ===== Script A: CHECK ONLY =====
import pandas as pd
from pathlib import Path

# แสดง URL เต็ม และไม่ตัดขึ้นบรรทัดใหม่
pd.set_option('display.max_colwidth', None)
pd.set_option('display.expand_frame_repr', False)

folder = Path(LOCAL_PATH_DATA)
bases = [
    'agro_company','consump_company','fincial_company','indus_company',
    'propcon_company','resourc_company','service_company','tech_company','na_company',
]

group_col1 = ['Return', 'Stat', 'Card Holder', 'Card Board', 'Adjust Price', 'Ratio', 'FinanStateSubmit']
group_col3 = ['BS', 'PL', 'CF']

all_results = []

for base in bases:
    file_path = folder / f"{base}.csv"
    print(f"\n===== {base} =====")
    try:
        df = pd.read_csv(file_path, header=0, encoding='utf-8-sig')

        # ทำความสะอาดชื่อคอลัมน์ + เติมคอลัมน์ที่อาจไม่มี
        df.columns = df.columns.astype(str).str.strip()
        for col in ['TableName', 'URL', 'Column1', 'Column3']:
            if col not in df.columns:
                df[col] = None

        # ทำความสะอาดค่า
        for col in ['TableName', 'URL', 'Column1', 'Column3']:
            df[col] = df[col].astype(str).str.strip()

        # เงื่อนไข
        mask_col1 = df['TableName'].isin(group_col1) & (df['Column1'] == 'ไม่มีข้อมูล')
        mask_col3 = df['TableName'].isin(group_col3) & (df['Column3'] == 'ไม่มีข้อมูล')
        cond = (mask_col1 | mask_col3)  # ไม่ต้อง reindex เพราะมาจาก df เดียวกัน

        df_out = df.loc[cond, ['TableName', 'URL']].drop_duplicates()

        if df_out.empty:
            print("(ไม่พบรายการที่เข้าเงื่อนไข)")
        else:
            print(df_out.to_string(index=False))

        if not df_out.empty:
            tmp = df_out.copy()
            tmp.insert(0, 'SourceFile', f"{base}.csv")
            all_results.append(tmp)

    except FileNotFoundError:
        print(f"ไม่พบไฟล์: {file_path}")
    except Exception as e:
        print(f"เกิดข้อผิดพลาดกับไฟล์ {file_path}: {e}")

if all_results:
    all_df = pd.concat(all_results, ignore_index=True).drop_duplicates()
    all_df.to_csv(folder / "_missing_data_summary.csv", index=False, encoding="utf-8-sig")

# ===== Script B: CASCADE DELETE BY URL =====
import pandas as pd
from pathlib import Path

# ----- พารามิเตอร์ -----
folder = Path(LOCAL_PATH_DATA)
bases = [
    'agro_company','consump_company','fincial_company','indus_company',
    'propcon_company','resourc_company','service_company','tech_company','na_company',
]
group_col1 = ['Return', 'Stat', 'Card Holder', 'Card Board', 'Adjust Price', 'Ratio', 'FinanStateSubmit']
group_col3 = ['BS', 'PL', 'CF']

APPLY_DELETE = True   # True = ลบจริง, False = Dry Run

summary = []

for base in bases:
    file_path = folder / f"{base}.csv"
    print(f"\n===== {base} =====")
    try:
        df = pd.read_csv(file_path, header=0, encoding='utf-8-sig')

        # ทำความสะอาดชื่อคอลัมน์ + เติมคอลัมน์ที่อาจไม่มี
        df.columns = df.columns.astype(str).str.strip()
        for col in ['TableName', 'URL', 'Column1', 'Column3']:
            if col not in df.columns:
                df[col] = None

        # ทำความสะอาดค่า (strip ช่องว่าง)
        for col in ['TableName', 'URL', 'Column1', 'Column3']:
            df[col] = df[col].astype(str).str.strip()

        # 1) หา URL ที่ "เข้าเงื่อนไข" (ไม่มีข้อมูลตามกติกาเดิม)
        mask_col1 = df['TableName'].isin(group_col1) & (df['Column1'] == 'ไม่มีข้อมูล')
        mask_col3 = df['TableName'].isin(group_col3) & (df['Column3'] == 'ไม่มีข้อมูล')
        cond = mask_col1 | mask_col3

        urls_to_delete = (
            df.loc[cond, 'URL']
              .dropna()
              .astype(str).str.strip()
              .drop_duplicates()
              .tolist()
        )

        print(f"> พบ URL ที่ต้องลบแบบ cascade: {len(urls_to_delete)} รายการ")
        if len(urls_to_delete) > 0:
            # 2) ลบ "ทุกแถว" ที่มี URL อยู่ในรายการนี้ (ไม่จำกัดด้วย cond แล้ว)
            cascade_mask = df['URL'].astype(str).str.strip().isin(urls_to_delete)
            n_to_delete = int(cascade_mask.sum())
            print(f"> จำนวนแถวที่มี URL ตรงกันและจะถูกลบ: {n_to_delete}")

            if APPLY_DELETE and n_to_delete > 0:
                df_after = df.loc[~cascade_mask].copy()
                df_after.to_csv(file_path, index=False, encoding='utf-8-sig')
                print(f"> ลบแล้ว: จาก {len(df)} เหลือ {len(df_after)}")
                summary.append((base, n_to_delete, len(urls_to_delete)))
            else:
                print("> DRY RUN หรือไม่มีอะไรให้ลบ")
                summary.append((base, 0, len(urls_to_delete)))
        else:
            print("(ไม่พบ URL ที่จะลบ)")
            summary.append((base, 0, 0))

    except FileNotFoundError:
        print(f"ไม่พบไฟล์: {file_path}")
        summary.append((base, -1, 0))
    except Exception as e:
        print(f"เกิดข้อผิดพลาดกับไฟล์ {file_path}: {e}")
        summary.append((base, -2, 0))

print("\n===== SUMMARY =====")
for base, n_rows, n_urls in summary:
    if n_rows == -1:
        print(f"{base}: ไม่พบไฟล์")
    elif n_rows == -2:
        print(f"{base}: เกิดข้อผิดพลาด")
    else:
        print(f"{base}: ลบ {n_rows} แถว (จาก URL เป้าหมาย {n_urls} รายการ)")

# ===== Script C: REFRESH DELETED URL BACK TO EACH SOURCE FILE (Multi-threaded) =====

# นำเข้าฟังก์ชัน worker_thread และ create_driver จาก Cell 6
# (ต้องมั่นใจว่า Cell 4 และ Cell 6 ถูกรันแล้วใน Session ปัจจุบัน)
import time
from pathlib import Path

import pandas as pd
import queue
import threading

# ---------- พารามิเตอร์หลัก ----------
folder = Path(LOCAL_PATH_DATA)
URL_LIST_PRIMARY   = folder / "_missing_data_summary.csv"
MAX_WORKERS_RETRY = 4 # ใช้จำนวน worker เท่าเดิม หรือปรับลดหากพบปัญหา Timeout

# ---------- ฟังก์ชันหลักสำหรับการ Refresh ----------

def retry_worker_thread(url_queue, retry_data):
    """
    Worker thread ที่ใช้สำหรับ Retry โดยใช้ Logic จาก worker_thread ใน Cell 6
    แต่ตัดส่วนการจัดการ result_list ที่เป็น global ออกไป
    """
    driver = None
    try:
        # ใช้ create_driver() และ Logic การ Scrape ทั้งหมดจาก Cell 6
        driver = create_driver()
        while True:
            try:
                url = url_queue.get(timeout=3)
            except queue.Empty:
                break
            
            try:
                driver.get(url)
                
                # 1. Smart Wait 1: รอให้โครงสร้างหลักโผล่มาก่อน
                WebDriverWait(driver, WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "company-info"))
                )

                # 2. Smart Wait 2: รอให้ Body/DOM พร้อมใช้งาน (แก้ Error scrollHeight)
                WebDriverWait(driver, WAIT_TIMEOUT).until(
                    lambda d: d.execute_script('return document.body != null && document.body.scrollHeight > 0;')
                )
                
                # สั่ง Scroll ลงมา
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                time.sleep(1) 
                
                # รอให้ข้อมูลโหลด
                time.sleep(5) # ใช้ 5 วินาที ตามที่ปรับปรุงล่าสุด
                
                # ดึง Source ทีเดียว
                html_source = driver.page_source
                
                # ส่งไปแกะข้อมูล (ใช้ parse_page_content ที่ปรับปรุงรูปแบบข้อมูลแล้ว)
                data = parse_page_content(html_source, url)
                
                # Thread-safe append
                if data:
                    retry_data.extend(data)
                    print(f"  ✓ Refreshed: {url.split('/')[-2]} | rows={len(data)}")
                else:
                    # Append empty list for logging/tracking purposes if necessary
                    print(f"  ⚠️ Refreshed (No Data): {url.split('/')[-2]}")
                
            except TimeoutException:
                print(f"  ❌ Error Timeout: {url}")
            except WebDriverException as e:
                print(f"  ❌ Error WebDriver: {url} ({e.msg.split('\\n')[0]})")
            except Exception as e:
                print(f"  ❌ Error Unexpected: {url} ({type(e).__name__}) - {e}")
            finally:
                url_queue.task_done()
                
    except Exception as e:
        print(f"🔥 Worker Initialization Error: {e}")
    finally:
        if driver:
            driver.quit()


# ---------- โหลดรายการ URL ที่ต้องรีเฟรช ----------

if not URL_LIST_PRIMARY.exists():
    print(f"[SKIP] ไม่พบไฟล์รายการ URL สำหรับรีเฟรช: '{URL_LIST_PRIMARY.name}'")
    
else:
    print(f"[INFO] ใช้ไฟล์รายการ URL: {URL_LIST_PRIMARY.name}")
    url_list_df = pd.read_csv(URL_LIST_PRIMARY, encoding="utf-8-sig")

    url_list_df.columns = url_list_df.columns.astype(str).str.strip()
    if "SourceFile" not in url_list_df.columns or "URL" not in url_list_df.columns:
        raise ValueError("ไฟล์รายการ URL ต้องมีคอลัมน์ 'SourceFile' และ 'URL'")

    url_list_df["SourceFile"] = url_list_df["SourceFile"].astype(str).str.strip()
    url_list_df["URL"]        = url_list_df["URL"].astype(str).str.strip()

    # ทำงานแยกตามไฟล์ปลายทาง
    grouped = url_list_df.groupby("SourceFile")
    
    for source_file, g in grouped:
        target_path = folder / source_file
        urls_to_retry = g["URL"].dropna().astype(str).str.strip().drop_duplicates().tolist()

        print(f"\n===== REFRESH -> {source_file} (Multi-threaded) =====")
        print(f"> จำนวน URL: {len(urls_to_retry)}")

        if not target_path.exists():
            print(f"[SKIP] ข้าม {source_file} เพราะยังไม่มีไฟล์ปลายทาง")
            continue
            
        # สร้าง Queue และ Result List สำหรับกลุ่มนี้
        q = queue.Queue()
        for u in urls_to_retry:
            q.put(u)
            
        retry_data = []
        threads = []
        num_workers = min(MAX_WORKERS_RETRY, len(urls_to_retry))
        
        # Start Workers
        for _ in range(num_workers):
            t = threading.Thread(target=retry_worker_thread, args=(q, retry_data))
            t.start()
            threads.append(t)
            
        # Wait for all threads
        for t in threads:
            t.join()
        
        # Merge Back to Source File
        if not retry_data:
            print("> ไม่มีข้อมูลใหม่จาก URL เหล่านี้ — ข้ามการบันทึก")
            continue

        # สร้าง DataFrame ใหม่จากผล scrape
        max_cols = max(len(r) for r in retry_data)
        new_cols = ["TableName", "URL"] + [f"Column{i+1}" for i in range(max_cols - 2)]
        new_df   = pd.DataFrame(retry_data, columns=new_cols)

        # โหลดไฟล์ปลายทาง
        old_df = pd.read_csv(target_path, header=0, encoding="utf-8-sig")
        old_df.columns = old_df.columns.astype(str).str.strip()

        # ปรับคอลัมน์ให้ “ยูเนียน” กัน
        all_cols = list(dict.fromkeys(list(old_df.columns) + list(new_df.columns)))
        for c in all_cols:
            if c not in old_df.columns:
                old_df[c] = pd.NA
            if c not in new_df.columns:
                new_df[c] = pd.NA

        # เรียงคอลัมน์ให้ตรงกัน
        old_df = old_df[all_cols]
        new_df = new_df[all_cols]

        # ต่อท้าย + กันซ้ำทั้งแถว (Drop Duplicates)
        before_rows = len(old_df)
        merged = pd.concat([old_df, new_df], ignore_index=True).drop_duplicates()
        after_rows = len(merged)

        # บันทึกกลับ
        merged.to_csv(target_path, index=False, encoding="utf-8-sig")
        print(f"> บันทึกกลับ {source_file}: เดิม {before_rows} แถว → ใหม่ {after_rows} แถว (+{after_rows - before_rows})")
        
    print("\n✅ เสร็จสิ้นการรีเฟรชข้อมูลด้วย Multi-threading")

# ===== Script D: CHECK ONLY =====
import pandas as pd
from pathlib import Path

# แสดง URL เต็ม และไม่ตัดขึ้นบรรทัดใหม่
pd.set_option('display.max_colwidth', None)
pd.set_option('display.expand_frame_repr', False)

folder = Path(LOCAL_PATH_DATA)
bases = [
    'agro_company','consump_company','fincial_company','indus_company',
    'propcon_company','resourc_company','service_company','tech_company','na_company',
]

group_col1 = ['Return', 'Stat', 'Card Holder', 'Card Board', 'Adjust Price', 'Ratio', 'FinanStateSubmit']
group_col3 = ['BS', 'PL', 'CF']

all_results = []

for base in bases:
    file_path = folder / f"{base}.csv"
    print(f"\n===== {base} =====")
    try:
        df = pd.read_csv(file_path, header=0, encoding='utf-8-sig')

        # ทำความสะอาดชื่อคอลัมน์ + เติมคอลัมน์ที่อาจไม่มี
        df.columns = df.columns.astype(str).str.strip()
        for col in ['TableName', 'URL', 'Column1', 'Column3']:
            if col not in df.columns:
                df[col] = None

        # ทำความสะอาดค่า
        for col in ['TableName', 'URL', 'Column1', 'Column3']:
            df[col] = df[col].astype(str).str.strip()

        # เงื่อนไข
        mask_col1 = df['TableName'].isin(group_col1) & (df['Column1'] == 'ไม่มีข้อมูล')
        mask_col3 = df['TableName'].isin(group_col3) & (df['Column3'] == 'ไม่มีข้อมูล')
        cond = (mask_col1 | mask_col3)  # ไม่ต้อง reindex เพราะมาจาก df เดียวกัน

        df_out = df.loc[cond, ['TableName', 'URL']].drop_duplicates()

        if df_out.empty:
            print("(ไม่พบรายการที่เข้าเงื่อนไข)")
        else:
            print(df_out.to_string(index=False))

        if not df_out.empty:
            tmp = df_out.copy()
            tmp.insert(0, 'SourceFile', f"{base}.csv")
            all_results.append(tmp)

    except FileNotFoundError:
        print(f"ไม่พบไฟล์: {file_path}")
    except Exception as e:
        print(f"เกิดข้อผิดพลาดกับไฟล์ {file_path}: {e}")

if all_results:
    all_df = pd.concat(all_results, ignore_index=True).drop_duplicates()

# ระบุพาธไฟล์ที่ต้องการลบ
target_file = LOCAL_PATH_DATA+'_missing_data_summary.csv'

# ตรวจสอบว่าไฟล์ มีอยู่หรือไม่
if os.path.exists(target_file):
    os.remove(target_file)  # ลบไฟล์
    print(f"ลบไฟล์ {target_file} เรียบร้อยแล้ว")
else:
    print(f"ไม่พบไฟล์ {target_file} ที่ต้องการลบ")