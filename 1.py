# FIXED KHÔNG GỞI ĐƯỢC NHIỀU PROMT 
import sys
import os
import json
import time
import threading
import queue
import base64
from PIL import Image, ImageDraw, ImageFont
import io
from datetime import datetime, timedelta
from pathlib import Path
import csv
import subprocess
import shutil
import hashlib
import re
import requests
from urllib.parse import unquote
from typing import List, Dict, Optional
from typing import List, Dict, Optional

FLOW_CONFIG = {
    "image_models": [
        {
            "key": "imagen_4",
            "app_label": "Imagen 4",
            "flow_candidates": ["Imagen 4", "Imagen"],
            "aliases": ["imagen 4", "imagen"],
        },
        {
            "key": "nano_banana",
            "app_label": "Nano Banana",
            "flow_candidates": ["Nano Banana", "Banana"],
            "aliases": ["nano banana", "banana"],
        },
        {
            "key": "nano_banana_pro",
            "app_label": "Nano Banana Pro",
            "flow_candidates": ["Nano Banana Pro", "Banana Pro"],
            "aliases": ["nano banana pro", "banana pro"],
        },
    ],
    "video_models": [
        {
            "key": "veo_3_1_fast",
            "app_label": "Veo 3.1 Fast",
            "flow_candidates": ["Veo 3.1 Fast", "Veo 3.1", "Fast"],
            "aliases": ["veo 3.1 fast"],
        },
        {
            "key": "veo_3_1_quality",
            "app_label": "Veo 3.1 Quality",
            "flow_candidates": ["Veo 3.1 Quality", "Veo 3.1", "Quality"],
            "aliases": ["veo 3.1 quality"],
        },
        {
            "key": "veo_3_1_fast_low",
            "app_label": "Veo 3.1 Fast [Lower Priority]",
            "flow_candidates": ["Veo 3.1 Fast [Lower Priority]"],
            "aliases": ["veo 3.1 fast [lower priority]"],
        },
    ],
    "aspect_ratios": [
        {
            "key": "16:9",
            "app_label": "16:9",
            "flow_candidates": [
                "16:9",
                "Khổ ngang (16:9)",
                "Landscape (16:9)",
                "Landscape",
                "Khổ ngang",
            ],
        },
        {
            "key": "9:16",
            "app_label": "9:16",
            "flow_candidates": [
                "9:16",
                "Khổ dọc (9:16)",
                "Portrait (9:16)",
                "Portrait",
                "Khổ dọc",
            ],
        },
    ],
    "output_counts": {
        "values": [1, 2, 3, 4],
        "image_candidates": {
            1: ["1", "1 images", "1 ảnh"],
            2: ["2", "2 images", "2 ảnh"],
            3: ["3", "3 images", "3 ảnh"],
            4: ["4", "4 images", "4 ảnh"],
        },
        "video_candidates": {
            1: ["1", "1 videos", "1 video", "1 câu trả lời", "1 kết quả"],
            2: ["2", "2 videos", "2 video", "2 câu trả lời", "2 kết quả"],
            3: ["3", "3 videos", "3 video", "3 câu trả lời", "3 kết quả"],
            4: ["4", "4 videos", "4 video", "4 câu trả lời", "4 kết quả"],
        },
    },
    "modes": {
        "image": {
            "code": "image",
            "flow_tab": "image",
            "side_nav_labels": [
                "tạo hình ảnh",
                "generate image",
                "generate images",
            ],
            "mode_labels": [
                "tạo hình ảnh",
                "generate image",
                "generate images",
            ],
        },
        "video": {
            "text": {
                "code": "text",
                "flow_tab": "video",
                "side_nav_labels": [
                    "từ văn bản sang video",
                    "text to video",
                    "văn bản thành video",
                ],
                "mode_labels": [
                    "từ văn bản sang video",
                    "text to video",
                    "văn bản thành video",
                ],
            },
            "image": {
                "code": "image",
                "flow_tab": "video",
                "side_nav_labels": [
                    "tạo video từ các khung hình",
                    "tạo video từ khung hình",
                    "create video from frames",
                    "image to video",
                    "images to video",
                    "image sequence",
                ],
                "mode_labels": [
                    "tạo video từ các khung hình",
                    "tạo video từ khung hình",
                    "create video from frames",
                    "image to video",
                    "images to video",
                    "image sequence",
                ],
            },
            "start_end": {
                "code": "start_end",
                "flow_tab": "video",
                "side_nav_labels": [
                    "tạo video từ các khung hình",
                    "tạo video từ khung hình",
                    "create video from frames",
                    "image to video",
                    "images to video",
                    "image sequence",
                ],
                "mode_labels": [
                    "tạo video từ các khung hình",
                    "tạo video từ khung hình",
                    "create video from frames",
                    "image to video",
                    "images to video",
                    "image sequence",
                ],
            },
            "reference": {
                "code": "reference",
                "flow_tab": "video",
                "side_nav_labels": [
                    "tạo video từ các thành phần",
                    "video from components",
                    "create video from components",
                ],
                "mode_labels": [
                    "tạo video từ các thành phần",
                    "video from components",
                    "create video from components",
                ],
            },
        },
    },
}

def read_prompts_from_file(file_path: str) -> List[str]:
    prompts: List[str] = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            if file_path.lower().endswith('.csv'):
                reader = csv.reader(f)
                for row in reader:
                    if not row:
                        continue
                    text = str(row[0]).strip()
                    if text:
                        prompts.append(text)
            else:
                for line in f:
                    text = line.strip()
                    if text:
                        prompts.append(text)
    except Exception:
        return []
    return prompts
# [NEW] Thêm thư viện Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import StaleElementReferenceException
import random
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QLabel, QLineEdit, QPushButton, QTextEdit, QComboBox,
    QSpinBox, QDoubleSpinBox, QCheckBox, QTableWidget, QTableWidgetItem, QFileDialog,
    QMessageBox, QProgressBar, QSplitter, QScrollArea, QRadioButton,
    QButtonGroup, QHeaderView, QFrame, QGridLayout, QStackedLayout, QDialog, QSizePolicy, QStyle,
    QGraphicsView, QGraphicsScene, QGraphicsPixmapItem
)
from PySide6.QtCore import Qt, QThread, Signal, QTimer, QSize, QUrl, qInstallMessageHandler
from PySide6.QtGui import QPixmap, QImage, QIcon, QColor, QFont, QPainter, QSyntaxHighlighter, QTextCharFormat
def _qt_silent_handler(mode, context, message):
    pass

qInstallMessageHandler(_qt_silent_handler)
class WorkerThread(QThread):
    progress_update = Signal(str, int, str, dict)
    log_message = Signal(str, str)
    task_complete = Signal(str, str, object)
    task_error = Signal(str, str)
    captcha_detected = Signal(int, str)
    
    def __init__(self):
        super().__init__()
        self.tasks = queue.Queue()
        self.is_running = True
        self.accounts = []
        self.current_task_sent_times = {}
        self.current_account = None
        self.hide_browser = False
        self.direct_project = False
        self.video_drivers = {}
        self.video_states = {}
        self.image_states = {}
        self.account_index = None
        self.prompt_delay = 0.5
        self.last_prompt_sent_at = 0.0
        self.download_queue = queue.Queue()
        self.downloader_thread = None
        self.media_queues = {"image": [], "video": []}
        self.media_seen_urls = {"image": set(), "video": set()}

        self.download_result_queue = queue.Queue()
        self.log_queue = queue.Queue()
        self.progress_queue = queue.Queue()

        self.video_network_failed_once = False
        self.video_download_strategy = "network_first"
        self.tile_seen_map = {"image": {}, "video": {}}

    def add_task(self, task_data):
        self.tasks.put(task_data)
        
    def stop(self):
        self.is_running = False
        try:
            while not self.tasks.empty():
                self.tasks.get_nowait()
        except queue.Empty:
            pass
        try:
            while not self.download_queue.empty():
                self.download_queue.get_nowait()
        except queue.Empty:
            pass
        self.tasks.put(None)
        try:
            self.download_queue.put(None)
        except Exception:
            pass
    
    def check_cancelled(self):
        if not self.is_running:
            raise Exception("Tác vụ đã bị dừng bởi người dùng")

    def detect_captcha_page(self, driver):
        try:
            html = driver.page_source
        except Exception:
            return False
        if not html:
            return False
        text = str(html).lower()
        keywords = [
            "recaptcha",
            "captcha",
            "i am not a robot",
            "i'm not a robot",
            "im not a robot",
            "i am not robot",
            "xác minh bạn không phải là rô-bốt",
            "xac minh ban khong phai la ro-bot",
            "xác minh bạn là người máy",
            "xac minh ban la nguoi may"
        ]
        for k in keywords:
            if k in text:
                return True
        return False

    def run(self):
        try:
            self.downloader_thread = threading.Thread(target=self.download_loop, daemon=True)
            self.downloader_thread.start()
            while self.is_running:
                self._drain_async_queues()
                try:
                    task = self.tasks.get(timeout=0.2)
                except queue.Empty:
                    continue

                if task is None:
                    if not self.is_running:
                        break
                    continue
                if not self.is_running:
                    break

                try:
                    self.process_task(task)
                except Exception as e:
                    self.log_message.emit(f"Worker error: {str(e)}", "error")
        finally:
            self.is_running = False
            try:
                self.download_queue.put(None)
            except Exception:
                pass
            if self.downloader_thread is not None:
                try:
                    self.downloader_thread.join(timeout=5)
                except Exception:
                    pass

            self._drain_async_queues()

            if self.video_drivers:
                for drv in self.video_drivers.values():
                    try:
                        drv.quit()
                    except Exception:
                        pass
                self.video_drivers.clear()
            self.video_states.clear()
            self.image_states.clear()

    def _drain_async_queues(self):
        while True:
            try:
                log_item = self.log_queue.get_nowait()
            except queue.Empty:
                break
            msg = log_item.get("message", "")
            level = log_item.get("type", "info")
            self.log_message.emit(msg, level)

        while True:
            try:
                prog_item = self.progress_queue.get_nowait()
            except queue.Empty:
                break
            task_id = prog_item.get("task_id")
            progress = prog_item.get("progress", 0)
            status = prog_item.get("status", "processing")
            self.progress_update.emit(task_id, progress, status, {})

        while True:
            try:
                res_item = self.download_result_queue.get_nowait()
            except queue.Empty:
                break

            kind = res_item.get("kind")
            task_id = res_item.get("task_id")

            if kind == "complete":
                prompt = res_item.get("prompt", "")
                results = res_item.get("results", [])
                self.task_complete.emit(task_id, prompt, results)
            elif kind == "error":
                error_message = res_item.get("error", "Unknown error")
                self.task_error.emit(task_id, error_message)
                self.log_message.emit(f"Lỗi tải media cho task {task_id}: {error_message}", "error")

    def download_loop(self):
        while self.is_running or not self.download_queue.empty():
            try:
                item = self.download_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if item is None:
                if not self.is_running:
                    break
                continue

            driver = item.get("driver")
            task_id = item.get("task_id")
            prompt = item.get("prompt")
            is_video = item.get("is_video", False)
            expected_count = item.get("expected_count", 1)
            log_prefix = item.get("log_prefix", "")
            download_dir = item.get("download_dir")
            sent_time = item.get("sent_time", 0)
            video_quality_1080 = item.get("video_quality_1080", False)
            account_index = item.get("account_index")

            if not driver or not task_id:
                continue

            try:
                self.wait_for_media_and_download(
                    driver=driver,
                    task_id=task_id,
                    prompt=prompt,
                    is_video=is_video,
                    expected_count=expected_count,
                    download_dir=download_dir,
                    log_prefix=log_prefix,
                    sent_time=sent_time,
                    video_quality_1080=video_quality_1080,
                    account_index=account_index
                )
            except Exception as e:
                self.download_result_queue.put({
                    "kind": "error",
                    "task_id": task_id,
                    "error": str(e)
                })

    def safe_click(self, driver, element, timeout=5):
        try:
            WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(element))
            element.click()
        except:
            driver.execute_script("arguments[0].click();", element)

    def select_dropdown_option(self, driver, label_texts, option_texts, timeout=8):
        try:
            if isinstance(label_texts, str):
                label_texts = [label_texts]
            if isinstance(option_texts, str):
                option_texts = [option_texts]

            search_roots = driver.find_elements(
                By.XPATH,
                "//div[@role='dialog' or @data-testid='settings-panel']"
            )
            root = search_roots[0] if search_roots else driver

            trigger_btn = None
            for lbl in label_texts:
                xpaths = [
                    f".//button[@role='combobox' and contains(., '{lbl}')]",
                    f".//label[contains(., '{lbl}')]/following::button[@role='combobox'][1]",
                    f".//span[contains(., '{lbl}')]/ancestor::button[@role='combobox'][1]"
                ]
                for xp in xpaths:
                    elems = root.find_elements(By.XPATH, xp)
                    for e in elems:
                        if e.is_displayed():
                            trigger_btn = e
                            break
                    if trigger_btn:
                        break
                if trigger_btn:
                    break

            if not trigger_btn:
                self.log_message.emit(
                    f"Không tìm thấy combobox cho label: {label_texts}",
                    "warning"
                )
                return False

            try:
                current_text = (trigger_btn.text or "").strip().lower()
            except Exception:
                current_text = ""

            for opt in option_texts:
                if opt and opt.strip() and opt.strip().lower() == current_text:
                    return True

            try:
                driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});",
                    trigger_btn
                )
            except Exception:
                pass

            try:
                driver.execute_script("arguments[0].click();", trigger_btn)
            except Exception:
                try:
                    trigger_btn.click()
                except Exception:
                    pass

            time.sleep(0.5)

            options = driver.find_elements(
                By.XPATH,
                "//div[@role='option'] | //li[@role='option'] | //div[@role='menuitem']"
            )

            if not options:
                self.log_message.emit(
                    f"Không thấy danh sách option cho label: {label_texts}",
                    "warning"
                )
                ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                return False

            target_opt = None

            for opt_elem in options:
                txt = (opt_elem.text or "").strip()
                if not txt:
                    continue
                if txt in option_texts:
                    target_opt = opt_elem
                    break

            if not target_opt:
                lowered = [str(v).strip().lower() for v in option_texts if str(v).strip()]
                for opt_elem in options:
                    txt = (opt_elem.text or "").strip().lower()
                    if not txt:
                        continue
                    for cand in lowered:
                        if cand in txt:
                            target_opt = opt_elem
                            break
                    if target_opt:
                        break

            if not target_opt:
                self.log_message.emit(
                    f"Không tìm thấy option phù hợp {option_texts} cho label {label_texts}",
                    "warning"
                )
                ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                return False

            try:
                driver.execute_script("arguments[0].click();", target_opt)
            except Exception:
                try:
                    target_opt.click()
                except Exception:
                    pass

            time.sleep(0.5)
            return True

        except Exception as e:
            self.log_message.emit(
                f"Lỗi select dropdown: {e}",
                "error"
            )
            try:
                ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            except Exception:
                pass
            return False

    def select_video_download_quality(self, driver, prefer_1080, timeout=4):
        end_time = time.time() + max(1.0, float(timeout or 1.0))
        prefer_1080 = bool(prefer_1080)
        while self.is_running and time.time() < end_time:
            try:
                elements = driver.find_elements(
                    By.XPATH,
                    "//*[contains(text(),'Ảnh GIF động') or "
                    "contains(text(),'Animated GIF') or "
                    "contains(text(),'GIF') or "
                    "contains(text(),'Kích thước gốc') or "
                    "contains(text(),'Original size') or "
                    "contains(text(),'original size') or "
                    "contains(text(),'720p') or "
                    "contains(text(),'1080p') or "
                    "contains(text(),'Đã tăng độ phân giải') or "
                    "contains(text(),'đã tăng độ phân giải') or "
                    "contains(text(),'Upscaled') or "
                    "contains(text(),'upscaled')]"
                )
            except Exception:
                elements = []

            visible_items = []
            for el in elements:
                try:
                    if el.is_displayed():
                        visible_items.append(el)
                except Exception:
                    continue

            if not visible_items:
                time.sleep(0.2)
                continue

            def get_text(el):
                try:
                    return ((el.text or "") + " " + (el.get_attribute("aria-label") or "")).lower()
                except Exception:
                    return ""

            target_1080 = None
            target_720 = None
            for el in visible_items:
                txt = get_text(el)
                if not txt:
                    continue
                if "gif" in txt:
                    continue
                if "1080" in txt or "đã tăng độ phân giải" in txt or "upscaled" in txt:
                    if not target_1080:
                        target_1080 = el
                if "720" in txt or "kích thước gốc" in txt or "original size" in txt:
                    if not target_720:
                        target_720 = el

            target = None
            if prefer_1080 and target_1080:
                target = target_1080
            elif not prefer_1080 and target_720:
                target = target_720
            elif target_720:
                target = target_720
            elif target_1080:
                target = target_1080

            if target:
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", target)
                except Exception:
                    pass
                try:
                    driver.execute_script("arguments[0].click();", target)
                except Exception:
                    try:
                        target.click()
                    except Exception:
                        pass
                return True

            time.sleep(0.2)

        return False

    def select_crop_ratio_and_save(self, driver, ratio_key, log_prefix=""):
        try:
            if ratio_key == "9:16":
                target_icon = "crop_9_16"
                target_label = "dọc"
            elif ratio_key == "16:9":
                target_icon = "crop_16_9"
                target_label = "ngang"
            else:
                return False

            try:
                buttons = driver.find_elements(
                    By.XPATH,
                    "//button[@role='combobox']"
                )
            except Exception:
                return False

            ratio_btn = None
            for btn in buttons:
                try:
                    if not btn.is_displayed():
                        continue
                    html = driver.execute_script("return arguments[0].outerHTML;", btn)
                    if target_icon in html and target_label in html.lower():
                        ratio_btn = btn
                        break
                except Exception:
                    continue

            if ratio_btn:
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", ratio_btn)
                    driver.execute_script("arguments[0].click();", ratio_btn)
                    time.sleep(0.4)
                except Exception:
                    pass

            try:
                save_btn = driver.find_element(
                    By.XPATH,
                    "//button[contains(., 'Cắt và lưu')]"
                )
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", save_btn)
                driver.execute_script("arguments[0].click();", save_btn)
                time.sleep(0.6)
                return True
            except Exception:
                return False

        except Exception as e:
            self.log_queue.put({
                "type": "error",
                "message": f"{log_prefix}Lỗi chọn tỉ lệ crop: {e}"
            })
            return False
    def select_main_flow_mode(self, driver, desired_labels, log_prefix=""):
        """
        Chọn combobox MODE chính (Tạo hình ảnh / Từ văn bản sang video / ...)
        nằm ngay phía trên textarea PINHOLE_TEXT_AREA_ELEMENT_ID.
        desired_labels: list text (hoặc 1 string) có thể xuất hiện trong nút / option.
        """
        try:
            def normalize(s):
                return (s or "").strip().lower()

            if isinstance(desired_labels, str):
                desired_labels = [desired_labels]
            desired_norm = [normalize(x) for x in desired_labels if x]

            primary_norm = desired_norm[0] if desired_norm else None
            secondary_norm = desired_norm[1:] if len(desired_norm) > 1 else []

            try:
                ta = driver.find_element(By.ID, "PINHOLE_TEXT_AREA_ELEMENT_ID")
            except Exception:
                tas = driver.find_elements(By.TAG_NAME, "textarea")
                ta = tas[-1] if tas else None

            if not ta:
                self.log_message.emit(
                    f"{log_prefix}Không tìm thấy ô nhập prompt để xác định combobox MODE.",
                    "warning"
                )
                return False

            try:
                container = ta.find_element(
                    By.XPATH,
                    "./ancestor::div[contains(@class,'sc-4d92f943-1') or "
                    "contains(@class,'sc-4d92f943-0')][1]"
                )
            except Exception:
                try:
                    container = ta.find_element(By.XPATH, "./ancestor::div[1]")
                except Exception:
                    container = None

            if not container:
                self.log_message.emit(
                    f"{log_prefix}Không tìm được container của combobox MODE.",
                    "warning"
                )
                return False

            try:
                mode_buttons = container.find_elements(By.XPATH, ".//button[@role='combobox']")
            except Exception:
                mode_buttons = []

            if not mode_buttons:
                try:
                    mode_buttons = driver.find_elements(
                        By.XPATH,
                        "//button[@role='combobox' and ("
                        "contains(., 'Tạo hình ảnh') or "
                        "contains(., 'Từ văn bản sang video') or "
                        "contains(., 'Tạo video từ các khung hình') or "
                        "contains(., 'Tạo video từ các thành phần') or "
                        "contains(., 'Generate image') or "
                        "contains(., 'Text to video'))]"
                    )
                except Exception:
                    mode_buttons = []

            if not mode_buttons:
                self.log_message.emit(
                    f"{log_prefix}Không thấy nút combobox MODE nào trên giao diện.",
                    "warning"
                )
                return False

            def get_button_text(btn):
                try:
                    return normalize((btn.text or "") + " " + (btn.get_attribute("aria-label") or ""))
                except Exception:
                    return ""

            mode_btn = None

            if primary_norm:
                for btn in mode_buttons:
                    try:
                        if not btn.is_displayed():
                            continue
                    except Exception:
                        continue
                    txt = get_button_text(btn)
                    if txt == primary_norm:
                        mode_btn = btn
                        break

            if not mode_btn and secondary_norm:
                for btn in mode_buttons:
                    try:
                        if not btn.is_displayed():
                            continue
                    except Exception:
                        continue
                    txt = get_button_text(btn)
                    for lbl in secondary_norm:
                        if lbl and txt == lbl:
                            mode_btn = btn
                            break
                    if mode_btn:
                        break

            if not mode_btn and desired_norm:
                for btn in mode_buttons:
                    try:
                        if not btn.is_displayed():
                            continue
                    except Exception:
                        continue
                    txt = get_button_text(btn)
                    for lbl in desired_norm:
                        if lbl and lbl in txt:
                            mode_btn = btn
                            break
                    if mode_btn:
                        break

            if not mode_btn:
                mode_btn = mode_buttons[0]

            try:
                current_text = normalize(
                    (mode_btn.text or "") + " " + (mode_btn.get_attribute("aria-label") or "")
                )
            except Exception:
                current_text = ""

            if desired_norm and any(lbl and lbl in current_text for lbl in desired_norm):
                return True

            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", mode_btn)
            except Exception:
                pass
            try:
                driver.execute_script("arguments[0].click();", mode_btn)
            except Exception:
                try:
                    mode_btn.click()
                except Exception:
                    pass

            time.sleep(0.4)

            try:
                option_elems = driver.find_elements(
                    By.XPATH,
                    "//div[@role='option'] | //li[@role='option'] | "
                    "//button[@role='menuitemradio'] | //div[@role='menuitem']"
                )
            except Exception:
                option_elems = []

            if not option_elems:
                self.log_message.emit(
                    f"{log_prefix}Không thấy danh sách chế độ sau khi mở combobox MODE.",
                    "warning"
                )
                return False

            def get_option_text(opt):
                try:
                    return normalize((opt.text or "") + " " + (opt.get_attribute("aria-label") or ""))
                except Exception:
                    return ""

            target_opt = None

            if primary_norm:
                for opt in option_elems:
                    try:
                        if not opt.is_displayed():
                            continue
                    except Exception:
                        continue
                    txt = get_option_text(opt)
                    if txt == primary_norm:
                        target_opt = opt
                        break

            if not target_opt and secondary_norm:
                for opt in option_elems:
                    try:
                        if not opt.is_displayed():
                            continue
                    except Exception:
                        continue
                    txt = get_option_text(opt)
                    for lbl in secondary_norm:
                        if lbl and txt == lbl:
                            target_opt = opt
                            break
                    if target_opt:
                        break

            if not target_opt and desired_norm:
                for opt in option_elems:
                    try:
                        if not opt.is_displayed():
                            continue
                    except Exception:
                        continue
                    txt = get_option_text(opt)
                    for lbl in desired_norm:
                        if lbl and lbl in txt:
                            target_opt = opt
                            break
                    if target_opt:
                        break

            if not target_opt:
                self.log_message.emit(
                    f"{log_prefix}Không tìm được option MODE phù hợp: {', '.join(desired_labels)}",
                    "warning"
                )
                try:
                    ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                except Exception:
                    pass
                return False

            try:
                driver.execute_script("arguments[0].click();", target_opt)
            except Exception:
                try:
                    target_opt.click()
                except Exception:
                    pass

            time.sleep(0.4)

            try:
                new_text = normalize(
                    (mode_btn.text or "") + " " + (mode_btn.get_attribute("aria-label") or "")
                )
            except Exception:
                new_text = ""

            if desired_norm and not any(lbl and lbl in new_text for lbl in desired_norm):
                self.log_message.emit(
                    f"{log_prefix}Đã click option nhưng không verify được MODE mới.",
                    "warning"
                )
                return False

            return True

        except Exception as e:
            self.log_message.emit(
                f"{log_prefix}Lỗi khi chọn combobox MODE: {e}",
                "error"
            )
            try:
                ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            except Exception:
                pass
            return False

    def _find_add_buttons(self, driver):
        try:
            elements = driver.find_elements(
                By.XPATH,
                "//button[.//i[contains(text(),'add')]]"
            )
        except Exception:
            elements = []
        result = []
        for e in elements:
            try:
                if e.is_displayed():
                    result.append(e)
            except Exception:
                continue
        return result

    def _upload_paths_via_input(self, input_elem, paths):
        if not input_elem or not paths:
            return False
        files = []
        for p in paths:
            if not p:
                continue
            ap = os.path.abspath(p)
            if os.path.exists(ap):
                files.append(ap)
        if not files:
            return False
        joined = "\n".join(files)
        try:
            input_elem.send_keys(joined)
            return True
        except Exception:
            return False

    def _find_file_input_near(self, driver, anchor):
        if anchor is not None:
            current = anchor
            for _ in range(4):
                try:
                    inputs = current.find_elements(By.XPATH, ".//input[@type='file']")
                except Exception:
                    inputs = []
                if inputs:
                    return inputs[0]
                try:
                    current = current.find_element(By.XPATH, "..")
                except Exception:
                    break
        try:
            inputs = driver.find_elements(By.XPATH, "//input[@type='file']")
        except Exception:
            inputs = []
        return inputs[0] if inputs else None

    def _upload_single_image_slot(self, driver, button_index, file_path, log_prefix=""):
        if not file_path or not os.path.exists(file_path):
            return False
        try:
            buttons = self._find_add_buttons(driver)
            if not buttons or button_index < 0 or button_index >= len(buttons):
                return False
            btn = buttons[button_index]
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            except Exception:
                pass
            self.safe_click(driver, btn, timeout=5)
            time.sleep(0.4)
            input_elem = self._find_file_input_near(driver, btn)
            if not input_elem:
                self.log_queue.put({
                    "type": "warning",
                    "message": f"{log_prefix}Không tìm thấy ô chọn file cho slot ảnh {button_index + 1}."
                })
                return False
            ok = self._upload_paths_via_input(input_elem, [file_path])
            if not ok:
                self.log_queue.put({
                    "type": "warning",
                    "message": f"{log_prefix}Upload ảnh slot {button_index + 1} thất bại."
                })
                return False

            ratio_key = "16:9"
            task_info = getattr(self, '_current_task_ratio', None)
            if task_info:
                ratio_key = str(task_info)

            self.select_crop_ratio_and_save(driver, ratio_key, log_prefix)
            return True
        except Exception:
            return False

    def _find_reference_upload_input(self, driver):
        try:
            container = driver.find_element(
                By.XPATH,
                "//div[contains(@class,'sc-fbea20b2-13') and (contains(.,'Tải lên') or contains(.,'Upload'))]"
            )
        except Exception:
            container = None
        if container is not None:
            try:
                inputs = container.find_elements(By.XPATH, ".//input[@type='file']")
            except Exception:
                inputs = []
            if inputs:
                return inputs[0]
        try:
            inputs = driver.find_elements(By.XPATH, "//input[@type='file']")
        except Exception:
            inputs = []
        return inputs[0] if inputs else None

    def _upload_reference_images(self, driver, paths, log_prefix=""):
        files = []
        for p in paths:
            if not p:
                continue
            ap = os.path.abspath(p)
            if os.path.exists(ap):
                files.append(ap)
        if not files:
            return False
        files = files[:3]
        try:
            buttons = self._find_add_buttons(driver)
            if buttons:
                btn = buttons[0]
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                except Exception:
                    pass
                self.safe_click(driver, btn, timeout=5)
                time.sleep(0.5)
        except Exception:
            pass
        input_elem = self._find_reference_upload_input(driver)
        if not input_elem:
            self.log_queue.put({
                "type": "warning",
                "message": f"{log_prefix}Không tìm thấy ô tải ảnh tham chiếu."
            })
            return False
        ok = self._upload_paths_via_input(input_elem, files)
        if not ok:
            self.log_queue.put({
                "type": "warning",
                "message": f"{log_prefix}Upload ảnh tham chiếu thất bại."
            })
        return ok

    def handle_video_inputs_by_mode(self, driver, mode_code, video_extra, log_prefix=""):
        code = (mode_code or "").strip()
        if not code or not isinstance(video_extra, dict):
            return
        if code == "image":
            path = video_extra.get("image_path")
            if not path:
                return
            self.log_queue.put({
                "type": "process",
                "message": f"{log_prefix}Đang upload ảnh đầu vào cho chế độ Hình ảnh → Video..."
            })
            self._upload_single_image_slot(driver, 0, path, log_prefix)
        elif code == "start_end":
            start_path = video_extra.get("start_image")
            end_path = video_extra.get("end_image")
            if not start_path or not end_path:
                return
            self.log_queue.put({
                "type": "process",
                "message": f"{log_prefix}Đang upload ảnh ĐẦU và CUỐI cho chế độ Đầu + cuối thành video..."
            })
            
            ratio_key = getattr(self, '_current_task_ratio', "16:9")
            
            self._upload_single_image_slot(driver, 0, start_path, log_prefix)
            time.sleep(0.3)
            self.select_crop_ratio_and_save(driver, ratio_key, log_prefix)
            
            self._upload_single_image_slot(driver, 1, end_path, log_prefix)
            time.sleep(0.3)
            self.select_crop_ratio_and_save(driver, ratio_key, log_prefix)
        elif code == "reference":
            paths = video_extra.get("ref_images") or []
            if not paths:
                return
            self.log_queue.put({
                "type": "process",
                "message": f"{log_prefix}Đang upload ảnh tham chiếu (tối đa 3) cho chế độ Tạo video tham chiếu..."
            })
            self._upload_reference_images(driver, paths, log_prefix)

    def collect_gcs_media_urls(self, driver, is_video, max_items, after_timestamp=None):
        """
        Thu thập URLs từ Network Log
        after_timestamp: Chỉ lấy request sau thời điểm này (milliseconds)
        """
        urls = []
        try:
            logs = driver.get_log("performance")
        except Exception:
            return []
        
        for entry in logs:
            try:
                msg = json.loads(entry.get("message", "{}")).get("message", {})
            except Exception:
                continue
            
            if msg.get("method") != "Network.responseReceived":
                continue
            
            # ✅ THÊM: Kiểm tra timestamp của request
            if after_timestamp:
                request_time = entry.get("timestamp", 0)  # milliseconds
                if request_time < after_timestamp:
                    continue  # Bỏ qua request cũ
            
            params = msg.get("params", {})
            resp = params.get("response", {})
            url = resp.get("url", "")
            mime = (resp.get("mimeType") or "").lower()
            
            if "ai-sandbox-videofx" not in url:
                continue
            
            if is_video:
                if not mime.startswith("video"):
                    continue
            else:
                if not mime.startswith("image"):
                    continue
            
            if url not in urls:
                urls.append(url)
            
            if len(urls) >= max_items:
                break
        
        return urls

    def download_urls_to_folder(self, urls, folder, is_video):
        count = 0
        session = requests.Session()
        for u in urls:
            if not self.is_running:
                break
            try:
                resp = session.get(u, stream=True, timeout=300)
            except Exception:
                continue
            if resp.status_code != 200:
                continue
            ct = (resp.headers.get("Content-Type") or "").lower()
            ext = ".bin"
            if "png" in ct:
                ext = ".png"
            elif "jpeg" in ct or "jpg" in ct:
                ext = ".jpg"
            elif "gif" in ct:
                ext = ".gif"
            elif "mp4" in ct:
                ext = ".mp4"
            elif "webm" in ct:
                ext = ".webm"
            elif is_video:
                ext = ".mp4"
            else:
                ext = ".png"
            out_path = os.path.join(folder, f"net_{count + 1}{ext}")
            try:
                with open(out_path, "wb") as f:
                    for chunk in resp.iter_content(8192):
                        if not self.is_running:
                            break
                        if chunk:
                            f.write(chunk)
            except Exception:
                continue
            count += 1
        return count

    def is_valid_media_file(self, path, is_video):
        try:
            ext = os.path.splitext(path)[1].lower()
            if is_video:
                if ext in [".mp4", ".webm", ".mov", ".mkv"]:
                    return os.path.getsize(path) > 0
                return False

            with open(path, "rb") as f:
                header = f.read(12)

            is_image = False
            if header.startswith(b"\x89PNG\r\n\x1a\n"):
                is_image = True
            elif header.startswith(b"\xff\xd8\xff"):
                is_image = True
            elif header[:6] in (b"GIF87a", b"GIF89a"):
                is_image = True
            elif header.startswith(b"RIFF") and header[8:12] == b"WEBP":
                is_image = True

            if not is_image:
                return False

            try:
                img = Image.open(path)
                w, h = img.size
                img.close()
            except Exception:
                return False

            if w < 256 or h < 256:
                return False

            if os.path.getsize(path) < 10 * 1024:
                return False

            return True
        except Exception:
            return False

    def download_video_with_ytdlp(self, url, out_dir, index):
        base_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        
        # --- SỬA LỖI WINERROR 193 ---
        # Tuyệt đối không tìm file yt-dlp.exe nữa vì file trong máy bạn đang bị lỗi
        # Luôn luôn gọi thông qua python module
        cmd_base = [sys.executable, "-m", "yt_dlp"]

        # Tìm ffmpeg (cái này vẫn cần thiết)
        ffmpeg_path = os.path.join(base_dir, "ffmpeg.exe")
        if not os.path.exists(ffmpeg_path):
            ffmpeg_path = "ffmpeg"

        base_name = f"flow_vid_{index}"
        out_path = os.path.join(out_dir, f"{base_name}.mp4")
        
        # In lệnh ra log để debug nếu cần
        # print(f"Executing: {cmd_base} output to {out_path}")

        cmd = cmd_base + [
            "--no-update",
            "--no-cache-dir",
            "--no-check-certificates",
            "--no-progress",
            "-f", "bv*+ba/b",
            "-o", out_path,
            "--ffmpeg-location", ffmpeg_path,
            url
        ]
        
        try:
            creation_flags = 0
            if sys.platform == "win32":
                creation_flags = subprocess.CREATE_NO_WINDOW
            
            res = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
                creationflags=creation_flags
            )
            
            # Nếu tải thất bại, log lại để biết nhưng không crash
            if res.returncode != 0:
                pass 
                # self.log_message.emit(f"Yt-dlp warning: {res.stderr}", "warning")

            if res.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                return out_path
        except Exception as e:
            self.log_message.emit(f"Lỗi yt-dlp subprocess: {e}", "error")
            
        # Fallback: Tải bằng requests nếu yt-dlp thất bại
        try:
            resp = requests.get(url, stream=True, timeout=300, verify=False)
            if resp.status_code == 200:
                with open(out_path, "wb") as f:
                    for chunk in resp.iter_content(8192):
                        if not self.is_running:
                            break
                        if chunk:
                            f.write(chunk)
                if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                    return out_path
        except Exception:
            return None
        return None

    def collect_video_sources(self, driver):
        urls = []
        try:
            videos = driver.find_elements(By.TAG_NAME, "video")
        except Exception:
            videos = []
        for v in videos:
            try:
                src = v.get_attribute("src") or ""
            except Exception:
                src = ""
            if src and src.startswith("http") and src not in urls:
                urls.append(src)
        return urls

    def collect_and_download_videos_flow(self, driver, out_dir, target_count, task_id, sent_time=None):
        seen_urls = set()
        global_seen = self.media_seen_urls.get("video", set())
        downloaded_files = []
        start_time = time.time()
        max_wait = 360
        next_index = 1

        while self.is_running and time.time() - start_time < max_wait and len(downloaded_files) < target_count:
            try:
                logs = driver.get_log("performance")
            except Exception:
                logs = []

            current_batch_urls = []

            for entry in logs:
                try:
                    msg = json.loads(entry.get("message", "{}")).get("message", {})
                except Exception:
                    continue

                if msg.get("method") != "Network.responseReceived":
                    continue

                if sent_time:
                    try:
                        entry_ts = entry.get("timestamp", 0) / 1000.0
                        if entry_ts < sent_time:
                            continue
                    except Exception:
                        pass

                params = msg.get("params", {})
                resp = params.get("response", {})
                url = resp.get("url", "")
                mime = (resp.get("mimeType") or "").lower()

                if "ai-sandbox-videofx" in url and ("video" in mime or "mpegurl" in mime or ".m3u8" in url):
                    if url not in seen_urls and url not in global_seen:
                        current_batch_urls.append(url)

            try:
                video_elements = driver.find_elements(By.TAG_NAME, "video")
            except Exception:
                video_elements = []

            for v in video_elements:
                try:
                    src = v.get_attribute("src")
                except Exception:
                    src = ""
                if not src:
                    continue
                if not src.startswith("http"):
                    continue
                if src in seen_urls or src in global_seen:
                    continue
                current_batch_urls.append(src)

            new_downloaded = False
            for src in current_batch_urls:
                if src in seen_urls or src in global_seen:
                    continue

                seen_urls.add(src)
                idx = next_index

                self.log_message.emit(f"Đang bắt link video {idx}...", "download")
                path = self.download_video_with_ytdlp(src, out_dir, idx)

                if path and os.path.exists(path) and os.path.getsize(path) > 0:
                    downloaded_files.append(path)
                    global_seen.add(src)
                    self.log_message.emit(f"✅✅ Đã tải video {idx}/{target_count}", "success")
                    next_index += 1
                    new_downloaded = True
                    if len(downloaded_files) >= target_count:
                        break
                else:
                    self.log_message.emit(f"⚠️ Tải thất bại video {idx}", "warning")

            self.media_seen_urls["video"] = global_seen

            if len(downloaded_files) >= target_count:
                break

            if len(downloaded_files) < target_count:
                try:
                    ActionChains(driver).send_keys(Keys.ARROW_RIGHT).perform()
                    self.log_message.emit("➡ Chuyển video kế tiếp...", "process")
                except Exception:
                    pass
                time.sleep(4)

        return downloaded_files

    def download_image_from_url(self, url: str, output_path: Path, driver=None, task_id: str = "", tile_index: int = 0, log_prefix: str = ""):
        prefix = log_prefix or ""
        try:
            session = requests.Session()
            if driver is not None:
                try:
                    selenium_cookies = driver.get_cookies()
                    for cookie in selenium_cookies:
                        session.cookies.set(cookie["name"], cookie["value"])
                except Exception:
                    pass

            headers = {
                "User-Agent": driver.execute_script("return navigator.userAgent;") if driver is not None else
                              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": driver.current_url if driver is not None else "https://labs.google/"
            }

            resp = session.get(url, headers=headers, stream=True, timeout=120)
            if resp.status_code != 200:
                self.log_queue.put({
                    "type": "error",
                    "message": f"{prefix}Task {task_id}: Lỗi tải ảnh tile {tile_index}, HTTP {resp.status_code}."
                })
                return None

            data = resp.content
            if not data or len(data) == 0:
                self.log_queue.put({
                    "type": "error",
                    "message": f"{prefix}Task {task_id}: Ảnh tile {tile_index} rỗng (0 byte)."
                })
                return None

            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "wb") as f:
                f.write(data)

            if output_path.exists() and output_path.stat().st_size > 0:
                return data

            self.log_queue.put({
                "type": "error",
                "message": f"{prefix}Task {task_id}: File ảnh tile {tile_index} sau khi lưu có kích thước 0."
            })
            return None

        except Exception as e:
            self.log_queue.put({
                "type": "error",
                "message": f"{prefix}Task {task_id}: Lỗi download_image_from_url tile {tile_index} – {e}"
            })
            return None

    def download_video_with_ytdlp_dom(self, url: str, output_dir: Path, task_id: str = "", tile_index: int = 0,
                                      quality: str = "1080p", log_prefix: str = ""):
        prefix = log_prefix or ""
        base_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        
        # --- SỬA LỖI WINERROR 193 ---
        # Luôn chạy bằng python module
        cmd_base = [sys.executable, "-m", "yt_dlp"]

        ffmpeg_path = os.path.join(base_dir, "ffmpeg.exe")
        if not os.path.exists(ffmpeg_path):
            ffmpeg_path = "ffmpeg"

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        fmt = "bv*+ba/b"
        if quality:
            q = quality.strip()
            if q == "1080p":
                fmt = "bestvideo[height<=1080]+bestaudio/best[height<=1080]/b"
            elif q == "720p":
                fmt = "bestvideo[height<=720]+bestaudio/best[height<=720]/b"

        base_name = f"flow_vid_task_{task_id}_tile_{tile_index}"
        out_tpl = str(output_dir / (base_name + ".%(ext)s"))

        cmd = cmd_base + [
            "--no-update",
            "--no-cache-dir",
            "--no-check-certificates",
            "--no-progress",
            "-f", fmt,
            "-o", out_tpl,
            "--ffmpeg-location", ffmpeg_path,
            url
        ]

        try:
            creation_flags = 0
            if sys.platform == "win32":
                creation_flags = subprocess.CREATE_NO_WINDOW
            res = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
                creationflags=creation_flags
            )
            
            if res.returncode == 0:
                for file in output_dir.glob(base_name + ".*"):
                    if file.is_file() and file.stat().st_size > 0:
                        try:
                            with open(file, "rb") as f:
                                return f.read()
                        except Exception:
                            break
            else:
                 # Log nhẹ warning
                 self.log_queue.put({
                    "type": "warning",
                    "message": f"{prefix}Task {task_id}: yt-dlp trả về code {res.returncode}. (Thử fallback)"
                })

        except Exception as e:
            self.log_queue.put({
                "type": "error",
                "message": f"{prefix}Task {task_id}: Lỗi yt-dlp tile {tile_index} – {e}"
            })

        # Fallback requests
        try:
            fallback_name = f"{base_name}_fallback.mp4"
            fallback_path = output_dir / fallback_name
            resp = requests.get(url, stream=True, timeout=300, verify=False)
            if resp.status_code == 200:
                with open(fallback_path, "wb") as f:
                    for chunk in resp.iter_content(8192):
                        if not self.is_running:
                            break
                        if chunk:
                            f.write(chunk)
                if fallback_path.exists() and fallback_path.stat().st_size > 0:
                    with open(fallback_path, "rb") as f:
                        return f.read()
        except Exception:
            pass

        self.log_queue.put({
            "type": "error",
            "message": f"{prefix}Task {task_id}: Không tải được video tile {tile_index} qua yt-dlp + fallback."
        })
        return None

    def extract_media_url_from_tile(self, tile, is_video: bool):
        storage_prefix = "https://storage.googleapis.com/ai-sandbox-videofx"
        try:
            if is_video:
                try:
                    videos = tile.find_elements(By.TAG_NAME, "video")
                except Exception:
                    videos = []
                for v in videos:
                    for attr in ["src", "data-src", "data-url"]:
                        try:
                            val = v.get_attribute(attr) or ""
                        except Exception:
                            val = ""
                        if val and val.startswith("http") and storage_prefix in val:
                            return val
                try:
                    sources = tile.find_elements(By.TAG_NAME, "source")
                except Exception:
                    sources = []
                for s in sources:
                    for attr in ["src", "data-src", "data-url"]:
                        try:
                            val = s.get_attribute(attr) or ""
                        except Exception:
                            val = ""
                        if val and val.startswith("http") and storage_prefix in val:
                            return val
            else:
                try:
                    imgs = tile.find_elements(By.TAG_NAME, "img")
                except Exception:
                    imgs = []
                for img in imgs:
                    for attr in ["src", "data-src"]:
                        try:
                            val = img.get_attribute(attr) or ""
                        except Exception:
                            val = ""
                        if val and val.startswith("http") and storage_prefix in val:
                            return val
                try:
                    pic_imgs = tile.find_elements(By.XPATH, ".//picture//img")
                except Exception:
                    pic_imgs = []
                for img in pic_imgs:
                    for attr in ["src", "data-src"]:
                        try:
                            val = img.get_attribute(attr) or ""
                        except Exception:
                            val = ""
                        if val and val.startswith("http") and storage_prefix in val:
                            return val
        except Exception:
            pass
        return None

    def _get_tile_seen_set(self, account_index, is_video, driver):
        kind = "video" if is_video else "image"
        key = account_index if account_index is not None else -1
        if kind not in self.tile_seen_map:
            self.tile_seen_map[kind] = {}
        kind_map = self.tile_seen_map[kind]
        entry = kind_map.get(key)
        if not entry or entry.get("driver_id") != id(driver):
            entry = {"driver_id": id(driver), "signatures": set()}
            kind_map[key] = entry
        return entry["signatures"]

    def _find_result_tiles_in_flow(self, driver, is_video):
        tiles = []
        seen = set()
        try:
            # Khối kết quả thường là các card có video/img bên trong
            containers = driver.find_elements(
                By.XPATH,
                "//div[.//video or .//img]"
            )
        except Exception:
            containers = []

        for container in containers:
            try:
                if not container.is_displayed():
                    continue
            except Exception:
                continue

            try:
                outer_html = driver.execute_script("return arguments[0].outerHTML;", container)
            except Exception:
                outer_html = ""

            # Chỉ nhận các container chứa link storage.googleapis.com/ai-sandbox-videofx
            if "ai-sandbox-videofx" not in outer_html:
                continue

            container_id = container.id
            if container_id in seen:
                continue
            seen.add(container_id)
            tiles.append(container)

        return tiles

    def _compute_tile_signature(self, driver, tile):
        try:
            outer_html = driver.execute_script("return arguments[0].outerHTML;", tile)
        except Exception:
            outer_html = ""
        try:
            rect = tile.rect or {}
        except Exception:
            rect = {}
        payload = f"{outer_html}|{int(rect.get('x', 0))}|{int(rect.get('y', 0))}|{int(rect.get('width', 0))}|{int(rect.get('height', 0))}"
        payload_bytes = payload.encode("utf-8", "ignore")
        return hashlib.sha1(payload_bytes).hexdigest()

    def _download_image_tile(self, driver, tile, download_dir, used_files, watch_start, task_id, tile_index, log_prefix):
        prefix = log_prefix or ""

        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", tile)
            time.sleep(0.3)
        except Exception:
            pass

        image_url = self.extract_media_url_from_tile(tile, is_video=False)
        if not image_url:
            self.log_queue.put({
                "type": "error",
                "message": f"{prefix}Task {task_id}: Không tìm thấy URL ảnh hợp lệ cho tile {tile_index} (DOM không có link storage.googleapis.com)."
            })
            return None

        global_seen = self.media_seen_urls.get("image", set())
        if image_url in global_seen:
            self.log_queue.put({
                "type": "warning",
                "message": f"{prefix}Task {task_id}: Bỏ qua tile {tile_index} vì URL ảnh đã được tải trước đó."
            })
            return None

        self.log_queue.put({
            "type": "download",
            "message": f"{prefix}Task {task_id}: Đang tải ảnh tile {tile_index} từ URL: {image_url[:80]}..."
        })

        ext = ".png"
        if ".jpg" in image_url or ".jpeg" in image_url:
            ext = ".jpg"
        elif ".webp" in image_url:
            ext = ".webp"

        out_dir = Path(download_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"flow_img_task_{task_id}_tile_{tile_index}{ext}"

        data = self.download_image_from_url(
            url=image_url,
            output_path=out_path,
            driver=driver,
            task_id=task_id,
            tile_index=tile_index,
            log_prefix=prefix
        )

        if not data:
            return None

        try:
            if len(data) < 1024:
                self.log_queue.put({
                    "type": "warning",
                    "message": f"{prefix}Task {task_id}: Ảnh tile {tile_index} có kích thước nhỏ (<1KB), có thể bị lỗi."
                })
        except Exception:
            pass

        global_seen.add(image_url)
        self.media_seen_urls["image"] = global_seen

        return data

    def detect_flow_error_banner(self, driver):
        try:
            elements = driver.find_elements(
                By.XPATH,
                "//*[@role='alert' or @aria-live='assertive' or @aria-live='polite' or contains(text(),'vi phạm') or contains(text(),'vi pham') or contains(text(),'không thể tạo') or contains(text(),'khong the tao') or contains(text(),'không thể xử lý') or contains(text(),'khong the xu ly') or contains(text(),'unable to generate') or contains(text(),'cannot generate') or contains(text(),'try again') or contains(text(),'content policy') or contains(text(),'violates') or contains(text(),'safety system')]"
            )
        except Exception:
            return None
        for el in elements:
            try:
                if not el.is_displayed():
                    continue
                text = el.text.strip()
            except Exception:
                continue
            if not text:
                continue
            lower = text.lower()
            keywords = [
                "vi phạm",
                "vi pham",
                "không thể",
                "khong the",
                "unable",
                "cannot",
                "violat",
                "policy",
                "safety",
                "try again"
            ]
            if any(k in lower for k in keywords):
                return text
        return None

    def _wait_for_new_video_url(self, driver, sent_time):
        if not hasattr(self, "video_download_strategy") or not self.video_download_strategy:
            self.video_download_strategy = "network_first"
        global_seen = self.media_seen_urls.get("video", set())
        end_time = time.time() + 90
        base_ts = sent_time if sent_time else time.time() - 600
        if base_ts < 0:
            base_ts = 0
        after_timestamp = int(base_ts * 1000)
        while self.is_running and time.time() < end_time:
            if self.video_download_strategy == "dom_first":
                order = ("dom", "network")
            else:
                order = ("network", "dom")
            url_found = None
            for method in order:
                if method == "network":
                    urls = self.collect_gcs_media_urls(
                        driver=driver,
                        is_video=True,
                        max_items=5,
                        after_timestamp=after_timestamp
                    )
                else:
                    urls = self.collect_video_sources(driver)
                for u in urls:
                    if not u or not u.startswith("http"):
                        continue
                    if u in global_seen:
                        continue
                    url_found = u
                    if method == "network":
                        self.video_download_strategy = "network_first"
                    else:
                        self.video_download_strategy = "dom_first"
                    break
                if url_found:
                    break
            if url_found:
                global_seen.add(url_found)
                self.media_seen_urls["video"] = global_seen
                return url_found
            time.sleep(1.0)
        return None

    def _handle_single_video_tile(self, driver, tile, download_dir, task_id, tile_index, sent_time, prefer_1080, log_prefix):
        prefix = log_prefix or ""
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", tile)
            time.sleep(0.5)
        except Exception:
            pass

        video_url = self.extract_media_url_from_tile(tile, is_video=True)
        if not video_url:
            self.log_queue.put({
                "type": "error",
                "message": f"{prefix}Task {task_id}: Không tìm thấy URL video hợp lệ cho tile {tile_index} (DOM không có link storage.googleapis.com)."
            })
            return None

        global_seen = self.media_seen_urls.get("video", set())
        if video_url in global_seen:
            self.log_queue.put({
                "type": "warning",
                "message": f"{prefix}Task {task_id}: Bỏ qua tile {tile_index} vì URL video đã được tải trước đó."
            })
            return None

        quality = "1080p" if prefer_1080 else "720p"

        self.log_queue.put({
            "type": "download",
            "message": f"{prefix}Task {task_id}: Đang tải video tile {tile_index} bằng yt-dlp ({quality}) từ URL: {video_url[:80]}..."
        })

        data = self.download_video_with_ytdlp_dom(
            url=video_url,
            output_dir=Path(download_dir),
            task_id=str(task_id),
            tile_index=tile_index,
            quality=quality,
            log_prefix=prefix
        )

        if not data:
            return None

        try:
            if len(data) < 10 * 1024:
                self.log_queue.put({
                    "type": "warning",
                    "message": f"{prefix}Task {task_id}: Video tile {tile_index} có kích thước nhỏ (<10KB), có thể bị lỗi."
                })
        except Exception:
            pass

        global_seen.add(video_url)
        self.media_seen_urls["video"] = global_seen

        return data

    def wait_for_media_and_download(self, driver, task_id, prompt, is_video, expected_count, download_dir, log_prefix="", sent_time=0, video_quality_1080=False, account_index=None):
        label = "video" if is_video else "ảnh"
        prefix = log_prefix or ""
        prefer_1080 = bool(video_quality_1080) if is_video else False

        self.log_queue.put({
            "type": "process",
            "message": f"{prefix}Bước 6/7: Đang chờ Flow tạo {label} cho task {task_id}..."
        })
        self.progress_queue.put({
            "task_id": task_id,
            "progress": 60,
            "status": "processing"
        })

        if not download_dir:
            raise Exception("Không xác định được thư mục tải về cho task.")
        download_dir = os.path.abspath(download_dir)
        if not os.path.isdir(download_dir):
            raise Exception(f"Thư mục tải về không tồn tại: {download_dir}")

        results = []
        used_files = set()
        if not is_video:
            try:
                for name in os.listdir(download_dir):
                    full_path = os.path.join(download_dir, name)
                    if os.path.isfile(full_path):
                        used_files.add(full_path)
            except Exception:
                pass

        start_time = time.time()
        watch_start = sent_time if sent_time else start_time
        max_wait = 600 if is_video else 240
        poll_interval = 1.0

        tile_seen = self._get_tile_seen_set(account_index, is_video, driver)

        while self.is_running and (time.time() - start_time) < max_wait and len(results) < expected_count:
            error_text = self.detect_flow_error_banner(driver)
            if error_text:
                raise Exception("FLOW_ERROR: " + error_text)
            try:
                tiles = self._find_result_tiles_in_flow(driver, is_video)
            except Exception:
                tiles = []

            new_tiles = []
            for tile in tiles:
                try:
                    sig = self._compute_tile_signature(driver, tile)
                except StaleElementReferenceException:
                    continue
                except Exception:
                    continue
                if not sig or sig in tile_seen:
                    continue
                tile_seen.add(sig)
                new_tiles.append(tile)

            if not new_tiles:
                time.sleep(poll_interval)
                continue

            for tile in new_tiles:
                if len(results) >= expected_count or not self.is_running:
                    break
                index_in_task = len(results) + 1
                try:
                    if is_video:
                        data = self._handle_single_video_tile(
                            driver=driver,
                            tile=tile,
                            download_dir=download_dir,
                            task_id=task_id,
                            tile_index=index_in_task,
                            sent_time=watch_start,
                            prefer_1080=prefer_1080,
                            log_prefix=prefix
                        )
                    else:
                        data = self._download_image_tile(
                            driver=driver,
                            tile=tile,
                            download_dir=download_dir,
                            used_files=used_files,
                            watch_start=watch_start,
                            task_id=task_id,
                            tile_index=index_in_task,
                            log_prefix=prefix
                        )
                except StaleElementReferenceException:
                    continue
                except Exception as e:
                    self.log_queue.put({
                        "type": "warning",
                        "message": f"{prefix}Task {task_id}: Lỗi khi xử lý tile {index_in_task}: {e}"
                    })
                    continue

                if not data:
                    continue

                results.append(data)
                self.log_queue.put({
                    "type": "download",
                    "message": f"{prefix}Task {task_id}: Đã tải {len(results)}/{expected_count} {label}."
                })
                prog = 60 + int(35 * len(results) / max(1, expected_count))
                if prog > 95:
                    prog = 95
                self.progress_queue.put({
                    "task_id": task_id,
                    "progress": prog,
                    "status": "processing"
                })

            if len(results) >= expected_count:
                break

            time.sleep(poll_interval)

        if not results:
            raise Exception("FLOW_ERROR: Không tìm thấy file ảnh/video được tạo sau thời gian chờ, Flow có thể đã trả lỗi hoặc từ chối prompt.")

        self.log_queue.put({
            "type": "success",
            "message": f"{prefix}Bước 7/7: Task {task_id} đã tải xong {len(results)} {label}."
        })
        self.progress_queue.put({
            "task_id": task_id,
            "progress": 100,
            "status": "completed"
        })
        self.download_result_queue.put({
            "kind": "complete",
            "task_id": task_id,
            "prompt": prompt,
            "results": results
        })

    def _inject_prompt_js(self, driver, wait, prompt, task_id, human_idx_text, log_prefix):
        prompt_text = (prompt or "").strip()
        if not prompt_text:
            raise Exception("Prompt trống.")

        try:
            try:
                prompt_box = driver.find_element(By.ID, "PINHOLE_TEXT_AREA_ELEMENT_ID")
            except Exception:
                tas = driver.find_elements(By.TAG_NAME, "textarea")
                prompt_box = tas[-1] if tas else None

            if not prompt_box:
                raise Exception("Không tìm thấy ô nhập prompt.")

            try:
                driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});",
                    prompt_box
                )
            except Exception:
                pass

            try:
                driver.execute_script("arguments[0].click();", prompt_box)
            except Exception:
                try:
                    prompt_box.click()
                except Exception:
                    pass

            time.sleep(0.1)
            try:
                driver.execute_script("arguments[0].value = '';", prompt_box)
            except Exception:
                try:
                    prompt_box.clear()
                except Exception:
                    try:
                        prompt_box.send_keys(Keys.CONTROL, "a")
                        prompt_box.send_keys(Keys.BACKSPACE)
                    except Exception:
                        pass

            inject_js = """
                const ta = arguments[0];
                const text = arguments[1];
                const proto = Object.getPrototypeOf(ta);
                const desc = Object.getOwnPropertyDescriptor(proto, 'value') || Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype,'value');
                if (desc && desc.set) {
                    desc.set.call(ta, text);
                } else {
                    ta.value = text;
                }
                ta.dispatchEvent(new Event('input', { bubbles: true }));
            """
            driver.execute_script(inject_js, prompt_box, prompt_text)

            time.sleep(0.1)
            current_val = driver.execute_script("return arguments[0].value;", prompt_box)
            if not current_val or not str(current_val).strip():
                prompt_box.send_keys(prompt_text)

            try:
                send_btn = driver.find_element(
                    By.XPATH,
                    "//button[contains(@aria-label, 'Gửi') or "
                    "contains(@aria-label, 'Send') or "
                    ".//i[contains(text(), 'arrow_upward')]"
                )
                try:
                    driver.execute_script("arguments[0].click();", send_btn)
                except Exception:
                    try:
                        send_btn.click()
                    except Exception:
                        pass
            except Exception:
                try:
                    prompt_box.send_keys(Keys.CONTROL, Keys.RETURN)
                except Exception:
                    driver.execute_script(
                        """
                        const e1 = new KeyboardEvent('keydown', {key:'Enter',ctrlKey:true,bubbles:true});
                        const e2 = new KeyboardEvent('keyup', {key:'Enter',ctrlKey:true,bubbles:true});
                        arguments[0].dispatchEvent(e1);
                        arguments[0].dispatchEvent(e2);
                        """,
                        prompt_box
                    )
        except Exception as e:
            raise Exception(f"Lỗi gửi prompt: {e}")

    def process_task(self, task):
        task_id = task["id"]
        prompt = task["prompt"]
        driver = None
        account_index = task.get("account_index", 0)
        count = task.get("count", 1)
        task_start_timestamp = time.time()
        task["start_time"] = task_start_timestamp
        model_name = task.get("model", "")
        ratio = task.get("ratio", "16:9")
        output_root = task.get("output_folder", os.path.abspath("output"))
        is_video = task.get("is_video", task_id.startswith("VID"))
        video_quality_1080 = task.get("video_quality_1080", False)
        video_mode = task.get("mode", None)
        video_extra = task.get("video_extra", {}) if is_video else {}

        order_index = task.get("order_index")
        total_tasks = task.get("total_tasks")
        if isinstance(order_index, int) and isinstance(total_tasks, int) and total_tasks > 0:
            prompt_prefix = f"[Prompt {order_index}/{total_tasks}] "
            human_idx_text = f"prompt thứ {order_index}/{total_tasks}"
        elif isinstance(order_index, int):
            prompt_prefix = f"[Prompt {order_index}] "
            human_idx_text = f"prompt thứ {order_index}"
        else:
            prompt_prefix = ""
            human_idx_text = "prompt"

        sub_folder = "Videos" if is_video else "Images"
        download_dir = os.path.join(output_root, sub_folder)
        os.makedirs(download_dir, exist_ok=True)

        max_attempts = 3

        try:
            self.progress_update.emit(task_id, 5, "processing", {})
            self.log_message.emit(
                f"{prompt_prefix}Bước 1/7: Chuẩn bị môi trường cho task {task_id}...",
                "process"
            )

            # ----- CHỌN ACCOUNT -----
            if self.accounts:
                if account_index < 0 or account_index >= len(self.accounts):
                    account_index = 0
                current_acc = self.accounts[account_index]
            else:
                current_acc = None

            if not current_acc:
                raise Exception("Không tìm thấy tài khoản nào để chạy.")

            acc_email = current_acc.get("email", "") or f"Account {account_index + 1}"
            acc_label = f"[Acc {account_index + 1} - {acc_email}] "
            project_link = str(current_acc.get("project_link", "") or "").strip()
            use_direct_project = bool(self.direct_project and project_link)
            log_prefix = f"{prompt_prefix}{acc_label}"

            self.log_message.emit(
                f"{log_prefix}Task {task_id}: Bắt đầu xử lý.",
                "process"
            )
            short_prompt = (prompt or "").replace("\n", " ").strip()
            if len(short_prompt) > 80:
                short_prompt = short_prompt[:77] + "..."
            self.log_message.emit(
                f"[Tài khoản {acc_email}] - Đang xử lý prompt: {short_prompt}",
                "process"
            )

            # ----- KHỞI TẠO / TÁI SỬ DỤNG CHROME -----
            reuse_video_driver = False
            if account_index in self.video_drivers:
                driver = self.video_drivers[account_index]
                reuse_video_driver = True
                try:
                    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
                        "behavior": "allow",
                        "downloadPath": os.path.abspath(download_dir)
                    })
                except Exception:
                    pass
            else:
                chrome_options = Options()
                if self.hide_browser:
                    chrome_options.add_argument("--headless=new")



                base_profile_dir = current_acc.get("profile_dir")
                if not base_profile_dir:
                    acc_id = current_acc.get("email", "default_user")
                    safe_folder_name = "".join([c for c in acc_id if c.isalnum()]) or f"user_{int(time.time())}"
                    base_profile_dir = os.path.join(
                        os.path.abspath(os.getcwd()),
                        "browser_profiles",
                        safe_folder_name
                    )

                profile_dir = base_profile_dir

                if not os.path.exists(profile_dir):
                    os.makedirs(profile_dir, exist_ok=True)

                prefs = {
                    "download.default_directory": os.path.abspath(download_dir),
                    "download.prompt_for_download": False,
                    "download.directory_upgrade": True,
                    "safebrowsing.enabled": True,
                    "profile.default_content_setting_values.automatic_downloads": 1
                }

                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument(f"--user-data-dir={profile_dir}")
                chrome_options.add_argument("--profile-directory=Default")
                chrome_options.add_argument("--disable-gpu")
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
                chrome_options.add_argument("--window-size=1440,900")
                chrome_options.add_argument("--log-level=3")
                
                # === FIX CHROME CRASH: Thêm các arguments ổn định hóa ===
                chrome_options.add_argument("--disable-extensions")
                chrome_options.add_argument("--disable-software-rasterizer")
                chrome_options.add_argument("--disable-features=VizDisplayCompositor")
                chrome_options.add_argument("--disable-background-networking")
                chrome_options.add_argument("--disable-default-apps")
                chrome_options.add_argument("--disable-sync")
                chrome_options.add_argument("--disable-translate")
                chrome_options.add_argument("--metrics-recording-only")
                chrome_options.add_argument("--mute-audio")
                chrome_options.add_argument("--no-first-run")
                chrome_options.add_argument("--safebrowsing-disable-auto-update")
                chrome_options.add_argument("--ignore-certificate-errors")
                chrome_options.add_argument("--ignore-ssl-errors")
                chrome_options.add_argument("--disable-popup-blocking")
                chrome_options.add_argument("--disable-infobars")
                chrome_options.add_argument("--remote-debugging-port=0")  # Port ngẫu nhiên tránh conflict
                # === END FIX ===

                user_agent = (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
                chrome_options.add_argument(f"user-agent={user_agent}")
                chrome_options.add_argument("--disable-blink-features=AutomationControlled")
                chrome_options.add_experimental_option(
                    "excludeSwitches",
                    ["enable-automation", "enable-logging"]
                )
                chrome_options.add_experimental_option("useAutomationExtension", False)
                chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

                try:
                    # 1. Tìm chromedriver.exe ngay tại thư mục chứa tool
                    base_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
                    local_driver = os.path.join(base_dir, "chromedriver.exe")
                    
                    if os.path.exists(local_driver):
                        # Nếu có file exe cạnh tool -> dùng nó (ổn định nhất)
                        service_obj = Service(executable_path=local_driver)
                    else:
                        # Nếu không có -> Tự tải (Dễ gây lỗi 193 nếu cache hỏng)
                        service_obj = Service(ChromeDriverManager().install())

                    # === FIX CHROME CRASH: Xóa lock file và retry ===
                    # Xóa các file lock trong profile để tránh conflict
                    lock_files = [
                        os.path.join(profile_dir, "SingletonLock"),
                        os.path.join(profile_dir, "SingletonSocket"),
                        os.path.join(profile_dir, "SingletonCookie"),
                        os.path.join(profile_dir, "Default", "SingletonLock"),
                        os.path.join(profile_dir, "Default", "SingletonSocket"),
                        os.path.join(profile_dir, "Default", "SingletonCookie"),
                    ]
                    for lock_file in lock_files:
                        try:
                            if os.path.exists(lock_file):
                                os.remove(lock_file)
                        except Exception:
                            pass
                    
                    # Thử khởi tạo Chrome với retry
                    max_retries = 3
                    last_error = None
                    driver = None
                    
                    for retry_attempt in range(max_retries):
                        try:
                            driver = webdriver.Chrome(service=service_obj, options=chrome_options)
                            break  # Thành công, thoát vòng lặp
                        except Exception as retry_error:
                            last_error = retry_error
                            error_str = str(retry_error).lower()
                            
                            # Nếu là lỗi Chrome crash/exit sớm -> thử lại
                            if "exited" in error_str or "crashed" in error_str or "session not created" in error_str:
                                self.log_message.emit(
                                    f"{log_prefix}Chrome crash lần {retry_attempt + 1}, đang thử lại...",
                                    "warning"
                                )
                                time.sleep(2)
                                
                                # Xóa lại lock files
                                for lock_file in lock_files:
                                    try:
                                        if os.path.exists(lock_file):
                                            os.remove(lock_file)
                                    except Exception:
                                        pass
                                
                                # Kill các process Chrome zombie có thể đang chiếm profile
                                try:
                                    if sys.platform == "win32":
                                        subprocess.run(
                                            ["taskkill", "/F", "/IM", "chrome.exe"],
                                            capture_output=True,
                                            creationflags=subprocess.CREATE_NO_WINDOW
                                        )
                                except Exception:
                                    pass
                                
                                time.sleep(1)
                                continue
                            else:
                                # Lỗi khác, không retry
                                break
                    
                    if driver is None:
                        if last_error:
                            raise last_error
                        else:
                            raise Exception("Không thể khởi tạo Chrome sau nhiều lần thử")
                    # === END FIX ===

                except Exception as e:
                    # Bắt đúng lỗi 193 để báo user cách fix
                    if "193" in str(e):
                        raise Exception("LỖI CHROME DRIVER (193): Hãy vào C:\\Users\\<User>\\ xóa thư mục '.wdm' rồi thử lại.")
                    # Thêm thông tin chi tiết hơn cho lỗi Chrome crash
                    if "exited" in str(e).lower() or "crashed" in str(e).lower():
                        raise Exception(
                            f"LỖI CHROME CRASH: Chrome đã thoát sớm. Nguyên nhân có thể: "
                            f"1) Profile Chrome đang mở ở nơi khác - đóng tất cả cửa sổ Chrome. "
                            f"2) Chrome và ChromeDriver không tương thích - cập nhật Chrome. "
                            f"3) Thử xóa thư mục browser_profiles. Chi tiết: {e}"
                        )
                    raise e
                try:
                    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
                        "behavior": "allow",
                        "downloadPath": os.path.abspath(download_dir)
                    })
                except Exception:
                    pass

                self.video_drivers[account_index] = driver

            # ----- QUẢN LÝ STATE VIDEO/IMAGE -----
            if is_video:
                state = self.video_states.get(account_index)
                if not state or state.get("driver_id") != id(driver):
                    self.video_states[account_index] = {
                        "initialized": False,
                        "driver_id": id(driver)
                    }
            else:
                state = self.image_states.get(account_index)
                if not state or state.get("driver_id") != id(driver):
                    self.image_states[account_index] = {
                        "initialized": False,
                        "driver_id": id(driver)
                    }

            if is_video:
                base_flow_url = "https://labs.google/fx/vi/tools/flow"
            else:
                base_flow_url = "https://labs.google/fx/vi/tools/flow"
                use_direct_project = False

            if is_video and use_direct_project:
                target_url = project_link
            else:
                target_url = base_flow_url

            self.progress_update.emit(task_id, 15, "processing", {})
            self.log_message.emit(
                f"{log_prefix}Bước 2/7: Đang mở Google Labs Flow ({'Video' if is_video else 'Image'})...",
                "network"
            )

            need_open_flow = True
            if is_video:
                v_state = self.video_states.get(account_index, {})
                if v_state.get("initialized") and v_state.get("driver_id") == id(driver):
                    need_open_flow = False
            else:
                i_state = self.image_states.get(account_index, {})
                if i_state.get("initialized") and i_state.get("driver_id") == id(driver):
                    need_open_flow = False

            if need_open_flow:
                driver.get(target_url)
            if is_video:
                try:
                    driver.get_log("performance")
                except Exception:
                    pass

            if need_open_flow:
                acc = current_acc or {}
                raw_cookie = acc.get("cookie", "")
                is_logged_in = False
                try:
                    if "Sign in" not in driver.title and "Đăng nhập" not in driver.title:
                        is_logged_in = True
                except:
                    pass

                if not is_logged_in and raw_cookie:
                    try:
                        cookies_list = []
                        raw_clean = raw_cookie.strip()
                        if raw_clean.startswith("{") or raw_clean.startswith("["):
                            json_data = json.loads(raw_clean)
                            cookies_list = (
                                json_data if isinstance(json_data, list)
                                else [json_data]
                            )
                        else:
                            for item in raw_clean.split(";"):
                                if "=" in item:
                                    name, value = item.split("=", 1)
                                    cookies_list.append({
                                        "name": name.strip(),
                                        "value": value.strip(),
                                        "domain": ".google.com"
                                    })

                        for c in cookies_list:
                            c_dict = {
                                "name": c.get("name"),
                                "value": c.get("value"),
                                "path": c.get("path", "/"),
                                "domain": c.get("domain", ".google.com"),
                                "secure": c.get("secure", True)
                            }
                            if "expiry" in c:
                                c_dict["expiry"] = int(c["expiry"])
                            try:
                                driver.add_cookie(c_dict)
                            except:
                                pass
                        driver.refresh()
                        time.sleep(3)
                    except Exception as e:
                        self.log_message.emit(f"Lỗi nạp cookie: {e}", "error")

                wait = WebDriverWait(driver, 60)
                time.sleep(3)
                current_url_check = driver.current_url
                if "ServiceLogin" in current_url_check or "signin" in current_url_check:
                    raise Exception(
                        "Cookie hết hạn hoặc chưa đăng nhập. Vui lòng login lại."
                    )
                if use_direct_project and "project" not in current_url_check:
                    driver.get(base_flow_url)
                    time.sleep(3)
                    use_direct_project = False

                if not use_direct_project:
                    try:
                        new_project_btn = WebDriverWait(driver, 8).until(
                            EC.element_to_be_clickable((
                                By.XPATH,
                                "//button[contains(., 'Dự án mới') or contains(., 'New project')]"
                            ))
                        )
                        self.safe_click(driver, new_project_btn)
                        time.sleep(2)
                    except:
                        pass

                try:
                    close_btn = driver.find_element(
                        By.XPATH,
                        "//button[@aria-label='Close' or @aria-label='Đóng']"
                    )
                    close_btn.click()
                except:
                    pass
            else:
                wait = WebDriverWait(driver, 60)

            if self.detect_captcha_page(driver):
                msg_captcha = "Chrome yêu cầu xác minh robot – vui lòng xác nhận thủ công và chạy lại."
                self.log_message.emit(msg_captcha, "warning")
                try:
                    if isinstance(account_index, int):
                        self.captcha_detected.emit(account_index, msg_captcha)
                except Exception:
                    pass
                raise Exception("FLOW_ERROR: " + msg_captcha)

            # ----- BƯỚC 3/4: CHỌN TAB + MODE + CẤU HÌNH -----
            if is_video:
                try:
                    mode_code = str(video_mode or "").strip()
                except Exception:
                    mode_code = ""
            else:
                mode_code = ""

            need_mode_and_config = True
            if is_video:
                v_state = self.video_states.get(account_index, {})
                if v_state.get("initialized") and v_state.get("driver_id") == id(driver):
                    prev_mode = v_state.get("mode", "")
                    prev_model = v_state.get("model", "")
                    prev_ratio = v_state.get("ratio", "")
                    prev_count = v_state.get("count", 1)
                    if (
                        prev_mode == mode_code and
                        prev_model == model_name and
                        prev_ratio == ratio and
                        int(prev_count) == int(count)
                    ):
                        need_mode_and_config = False
            else:
                i_state = self.image_states.get(account_index, {})
                if i_state.get("initialized") and i_state.get("driver_id") == id(driver):
                    prev_model = i_state.get("model", "")
                    prev_ratio = i_state.get("ratio", "")
                    prev_count = i_state.get("count", 1)
                    if (
                        prev_model == model_name and
                        prev_ratio == ratio and
                        int(prev_count) == int(count)
                    ):
                        need_mode_and_config = False

            if need_mode_and_config:
                self.progress_update.emit(task_id, 30, "processing", {})
                self.log_message.emit(
                    f"{log_prefix}Bước 3/7: Chọn tab {'Video' if is_video else 'Image'} và chế độ sinh nội dung...",
                    "process"
                )
                try:
                    def normalize(s):
                        return (s or "").strip().lower()

                    # --- 3.1 Chọn tab VIDEO / IMAGES trên cùng ---
                    try:
                        top_tabs = driver.find_elements(
                            By.XPATH,
                            "//button[contains(., 'Videos') or contains(., 'Video') or "
                            "contains(., 'Images') or contains(., 'Image') or "
                            "contains(., 'Hình ảnh') or contains(., 'Hình Ảnh') or "
                            "contains(., 'Ảnh')]"
                        )
                    except Exception:
                        top_tabs = []

                    for btn in top_tabs:
                        try:
                            txt = normalize(
                                (btn.text or "") + " " + (btn.get_attribute("aria-label") or "")
                            )
                            is_video_tab = any(k in txt for k in ["video", "videos"])
                            is_image_tab = any(k in txt for k in ["image", "images", "hình ảnh", "hình  ảnh", "ảnh"])
                            if is_video and not is_video_tab:
                                continue
                            if (not is_video) and not is_image_tab:
                                continue
                            aria_pressed = (btn.get_attribute("aria-pressed") or "").lower()
                            aria_selected = (btn.get_attribute("aria-selected") or "").lower()
                            if aria_pressed == "true" or aria_selected == "true":
                                break
                            try:
                                driver.execute_script(
                                    "arguments[0].scrollIntoView({block: 'center'});",
                                    btn
                                )
                            except Exception:
                                pass
                            try:
                                driver.execute_script("arguments[0].click();", btn)
                            except Exception:
                                try:
                                    btn.click()
                                except Exception:
                                    pass
                            time.sleep(0.6)
                            break
                        except Exception:
                            continue

                    # --- 3.2 Chọn mục bên trái (side nav) nếu có ---
                    side_nav_map = {}
                    if is_video:
                        for key, cfg in FLOW_CONFIG["modes"]["video"].items():
                            side_nav_map[key] = cfg["side_nav_labels"]
                    else:
                        side_nav_map = {
                            "image": FLOW_CONFIG["modes"]["image"]["side_nav_labels"]
                        }

                    desired_side_labels = []
                    if is_video:
                        key = mode_code if mode_code in side_nav_map else "text"
                        desired_side_labels = side_nav_map.get(key, [])
                    else:
                        desired_side_labels = side_nav_map.get("image", [])

                    if desired_side_labels:
                        normalized_targets = [normalize(x) for x in desired_side_labels if x]
                        side_buttons = driver.find_elements(
                            By.XPATH,
                            "//button | //div[@role='tab'] | //div[@role='button'] | //a"
                        )
                        for btn in side_buttons:
                            try:
                                if not btn.is_displayed():
                                    continue
                                txt = normalize(btn.text)
                                aria = normalize(btn.get_attribute("aria-label"))
                                combo = f"{txt} {aria}".strip()
                                matched = False
                                for target in normalized_targets:
                                    if target and target in combo:
                                        matched = True
                                        break
                                if not matched:
                                    continue
                                aria_pressed = (btn.get_attribute("aria-pressed") or "").lower()
                                aria_selected = (btn.get_attribute("aria-selected") or "").lower()
                                if aria_pressed == "true" or aria_selected == "true":
                                    break
                                try:
                                    driver.execute_script(
                                        "arguments[0].scrollIntoView({block: 'center'});",
                                        btn
                                    )
                                except Exception:
                                    pass
                                try:
                                    driver.execute_script("arguments[0].click();", btn)
                                except Exception:
                                    try:
                                        btn.click()
                                    except Exception:
                                        pass
                                time.sleep(0.8)
                                break
                            except Exception:
                                continue

                    # --- 3.3 CHỌN COMBOBOX MODE CHÍNH Ở NGAY TRÊN Ô PROMPT ---
                    video_mode_map = {}
                    for key, cfg in FLOW_CONFIG["modes"]["video"].items():
                        video_mode_map[key] = cfg["mode_labels"]
                    image_mode_map = {
                        "image": FLOW_CONFIG["modes"]["image"]["mode_labels"]
                    }

                    if is_video:
                        mode_key = mode_code if mode_code in video_mode_map else "text"
                        desired_labels = video_mode_map[mode_key]
                    else:
                        desired_labels = image_mode_map["image"]

                    self.select_main_flow_mode(
                        driver,
                        desired_labels,
                        log_prefix=log_prefix
                    )

                except Exception:
                    pass

                # --- 4. CẤU HÌNH MODEL / COUNT / RATIO ---
                self.progress_update.emit(task_id, 40, "processing", {})
                self.log_message.emit(
                    f"{log_prefix}Bước 4/7: Thiết lập mô hình, số lượng kết quả và tỷ lệ khung hình...",
                    "process"
                )
                try:
                    try:
                        settings_btn = wait.until(
                            EC.element_to_be_clickable((
                                By.XPATH,
                                "//button[.//i[contains(text(),'tune')] or "
                                "contains(@aria-label, 'Settings') or "
                                "contains(@aria-label, 'Cài đặt')]"
                            ))
                        )
                        if settings_btn.get_attribute("aria-expanded") != "true":
                            driver.execute_script(
                                "arguments[0].scrollIntoView({block: 'center'});",
                                settings_btn
                            )
                            driver.execute_script("arguments[0].click();", settings_btn)
                            time.sleep(0.6)
                    except:
                        pass

                    ui_candidates = []
                    safe_name = (model_name or "").strip().lower()
                    if safe_name:
                        if is_video:
                            for cfg in FLOW_CONFIG["video_models"]:
                                labels = [cfg["app_label"]] + cfg.get("aliases", [])
                                labels_lower = [s.lower() for s in labels]
                                matched = False
                                for lbl in labels_lower:
                                    if safe_name == lbl:
                                        matched = True
                                        break
                                if matched:
                                    ui_candidates = cfg["flow_candidates"]
                                    break
                            if not ui_candidates:
                                ui_candidates = [model_name]
                        else:
                            for cfg in FLOW_CONFIG["image_models"]:
                                labels = [cfg["app_label"]] + cfg.get("aliases", [])
                                labels_lower = [s.lower() for s in labels]
                                matched = False
                                for lbl in labels_lower:
                                    if safe_name == lbl:
                                        matched = True
                                        break
                                if matched:
                                    ui_candidates = cfg["flow_candidates"]
                                    break
                            if not ui_candidates:
                                ui_candidates = [model_name]

                        self.select_dropdown_option(
                            driver,
                            ["Model", "Mô hình", "Mô hình đầu ra"],
                            ui_candidates
                        )

                    count_str = str(count).strip()
                    if count_str:
                        if is_video:
                            candidates_map = FLOW_CONFIG["output_counts"]["video_candidates"]
                        else:
                            candidates_map = FLOW_CONFIG["output_counts"]["image_candidates"]
                        try:
                            count_int = int(count)
                        except Exception:
                            count_int = None
                        if isinstance(count_int, int) and count_int in candidates_map:
                            count_candidates = candidates_map[count_int]
                        else:
                            if is_video:
                                count_candidates = [
                                    count_str,
                                    f"{count_str} videos",
                                    f"{count_str} video",
                                    f"{count_str} câu trả lời",
                                    f"{count_str} kết quả"
                                ]
                            else:
                                count_candidates = [
                                    count_str,
                                    f"{count_str} images",
                                    f"{count_str} ảnh"
                                ]
                        self.select_dropdown_option(
                            driver,
                            [
                                "Sample count",
                                "Số lượng",
                                "Count",
                                "Câu trả lời",
                                "Câu trả lời đầu ra cho mỗi lệnh"
                            ],
                            count_candidates
                        )

                    ratio_str = str(ratio).strip()
                    if ratio_str:
                        ratio_candidates = []
                        for cfg in FLOW_CONFIG["aspect_ratios"]:
                            if cfg["key"] == ratio_str or cfg["app_label"] == ratio_str:
                                ratio_candidates = cfg["flow_candidates"]
                                break
                        if not ratio_candidates:
                            ratio_candidates = [ratio_str]
                        self.select_dropdown_option(
                            driver,
                            ["Aspect ratio", "Tỷ lệ khung hình"],
                            ratio_candidates
                        )

                    time.sleep(0.5)
                    try:
                        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                    except:
                        pass
                except:
                    pass

                # Lưu state để lần sau khỏi chọn lại nếu cùng cấu hình
                if is_video:
                    v_state = self.video_states.get(account_index, {})
                    v_state["initialized"] = True
                    v_state["driver_id"] = id(driver)
                    v_state["mode"] = mode_code
                    v_state["model"] = model_name
                    v_state["ratio"] = ratio
                    v_state["count"] = int(count)
                    self.video_states[account_index] = v_state
                else:
                    i_state = self.image_states.get(account_index, {})
                    i_state["initialized"] = True
                    i_state["driver_id"] = id(driver)
                    i_state["model"] = model_name
                    i_state["ratio"] = ratio
                    i_state["count"] = int(count)
                    self.image_states[account_index] = i_state
            else:
                if is_video:
                    self.log_message.emit(
                        f"{log_prefix}Bước 3-4/7: Giữ nguyên Mode và cấu hình video như lần trước (không đổi).",
                        "process"
                    )
                else:
                    self.log_message.emit(
                        f"{log_prefix}Bước 3-4/7: Giữ nguyên cấu hình hình ảnh như lần trước (không đổi).",
                        "process"
                    )
            if is_video and video_extra:
                try:
                    self.handle_video_inputs_by_mode(driver, mode_code, video_extra, log_prefix)
                except Exception as e:
                    self.log_message.emit(
                        f"{log_prefix}Lỗi khi upload dữ liệu đầu vào video: {e}",
                        "warning"
                    )

            # ----- BƯỚC 5: GỬI PROMPT (KHÔNG CHỜ TẠO XONG / DOWNLOAD) -----
            result_label = "video" if is_video else "ảnh"
            self.progress_update.emit(task_id, 50, "processing", {})

            sent_ok = False
            for attempt_index in range(1, max_attempts + 1):
                if attempt_index == 1:
                    self.log_message.emit(
                        f"{log_prefix}Bước 5/7: Đang chuẩn bị gửi {human_idx_text} lên Flow...",
                        "process"
                    )
                else:
                    self.log_message.emit(
                        f"{log_prefix}Bước 5/7: Thử lại lần {attempt_index}/{max_attempts} khi gửi {human_idx_text}...",
                        "warning"
                    )

                try:
                    try:
                        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                    except Exception:
                        pass

                    delay_value = float(getattr(self, "prompt_delay", 0.0) or 0.0)
                    if delay_value > 0:
                        now_ts = time.time()
                        wait_need = (self.last_prompt_sent_at or 0.0) + delay_value - now_ts
                        if wait_need > 0:
                            time.sleep(wait_need)

                    self._inject_prompt_js(driver, wait, prompt, task_id, human_idx_text, log_prefix)

                    self.log_message.emit(
                        f"{log_prefix}Task {task_id}: ĐÃ GỬI {human_idx_text} lên Flow, chờ Flow nhận job...",
                        "network"
                    )

                    sent_timestamp = time.time()
                    task["sent_time"] = sent_timestamp
                    self.last_prompt_sent_at = sent_timestamp

                    time.sleep(0.05)
                    sent_ok = True
                    break
                except Exception as e:
                    if attempt_index >= max_attempts:
                        raise Exception(f"Lỗi nhập/gửi prompt: {e}")
                    time.sleep(0.2)

            if not sent_ok:
                raise Exception("Không thể gửi prompt sau nhiều lần thử.")

            # ✅ Lưu timestamp để lọc video
            sent_time = task.get("sent_time", time.time())
            self.current_task_sent_times[task_id] = sent_time

            download_task = {
                "driver": driver,
                "account_index": account_index,
                "task_id": task_id,
                "prompt": prompt,
                "is_video": is_video,
                "expected_count": int(count) if count else 1,
                "log_prefix": log_prefix,
                "sent_time": sent_time,
                "download_dir": download_dir,
                "video_quality_1080": video_quality_1080
            }
            self.download_queue.put(download_task)

        except Exception as e:
            err_msg = str(e) if e is not None else ""
            err_lower = err_msg.lower()

            # Nhận diện riêng trường hợp Chrome đã bị đóng / mất kết nối
            chrome_closed_keywords = [
                "chrome not reachable",
                "disconnected",
                "received shutdown signal",
                "cannot determine loading status",
                "chrome failed to start",
                "invalid session id",
                "no such window",
                "target frame detached"
            ]

            is_chrome_closed = any(k in err_lower for k in chrome_closed_keywords)

            if is_chrome_closed:
                try:
                    account_index = task.get("account_index", None)
                except Exception:
                    account_index = None

                self.task_error.emit(task_id, "Trình duyệt Chrome đã bị tắt – dừng toàn bộ task liên quan.")

                # Ghi log ngắn gọn (AccountManager sẽ log 1 dòng chuẩn)
                self.log_message.emit(
                    f"Chrome của account_index={account_index} đã bị tắt trong khi chạy task {task_id}.",
                    "error"
                )

            else:
                self.task_error.emit(task_id, err_msg)
                self.log_message.emit(f"Lỗi Task {task_id}: {err_msg}", "error")
        finally:
            pass

class GeminiAPIManager:
    """Class quản lý API key và gọi Gemini API (sử dụng class methods)"""
    _api_key = ""
    _model = "gemini-2.5-flash"
    _genai_module = None  # Cache the import
    
    @classmethod
    def _get_genai(cls):
        """Lazy import và cache google.generativeai module"""
        if cls._genai_module is None:
            try:
                import google.generativeai as genai
                cls._genai_module = genai
            except ImportError:
                return None
        return cls._genai_module
    
    @classmethod
    def set_api_key(cls, key):
        cls._api_key = key.strip()
    
    @classmethod
    def get_api_key(cls):
        return cls._api_key
    
    @classmethod
    def set_model(cls, model):
        cls._model = model
    
    @classmethod
    def get_model(cls):
        return cls._model
    
    @classmethod
    def call_gemini(cls, prompt, content=""):
        """Gọi Gemini API để tạo nội dung"""
        if not cls._api_key:
            return None, "Chưa cài đặt API Key Gemini. Vui lòng vào tab Cài Đặt API để nhập key."
        
        genai = cls._get_genai()
        if genai is None:
            return None, "Thư viện google-generativeai chưa được cài đặt. Vui lòng cài đặt bằng lệnh: pip install google-generativeai"
        
        try:
            genai.configure(api_key=cls._api_key)
            
            model = genai.GenerativeModel(cls._model)
            
            if content:
                full_prompt = f"{prompt}\n\n{content}"
            else:
                full_prompt = prompt
            
            response = model.generate_content(full_prompt)
            return response.text, None
        except Exception as e:
            return None, f"Lỗi gọi Gemini API: {str(e)}"
    
    @classmethod
    def analyze_youtube(cls, youtube_url, prompt=""):
        """Phân tích video YouTube với Gemini"""
        if not cls._api_key:
            return None, "Chưa cài đặt API Key Gemini. Vui lòng vào tab Cài Đặt API để nhập key."
        
        genai = cls._get_genai()
        if genai is None:
            return None, "Thư viện google-generativeai chưa được cài đặt. Vui lòng cài đặt bằng lệnh: pip install google-generativeai"
        
        try:
            genai.configure(api_key=cls._api_key)
            
            model = genai.GenerativeModel(cls._model)
            
            default_prompt = """Hãy xem video này và tạo ra một kịch bản để tạo video AI có thể truyền tải đúng nội dung.
Chia thành các kịch bản theo thứ tự phù hợp với video 8 giây.
Mỗi kịch bản là 1 dòng.
Kết quả là văn bản thuần túy, KHÔNG thêm lời dẫn, chú thích, markdown, in đậm/in nghiêng."""
            
            if prompt:
                full_prompt = prompt
            else:
                full_prompt = default_prompt
            
            # Gemini 1.5+ có thể xử lý YouTube URLs trực tiếp
            # Gửi URL như một phần của content
            response = model.generate_content([youtube_url, full_prompt])
            return response.text, None
        except Exception as e:
            error_msg = str(e)
            # Cung cấp hướng dẫn hữu ích nếu có lỗi
            if "not supported" in error_msg.lower() or "invalid" in error_msg.lower():
                return None, f"Lỗi: Model {cls._model} có thể không hỗ trợ phân tích video trực tiếp. Thử đổi sang gemini-1.5-flash-latest hoặc gemini-1.5-pro-latest. Chi tiết: {error_msg}"
            return None, f"Lỗi phân tích YouTube: {error_msg}"


class GeminiWorker(QThread):
    """Worker thread để gọi Gemini API không block UI"""
    finished = Signal(str, str)  # (result, error)
    
    def __init__(self, task_type, **kwargs):
        super().__init__()
        self.task_type = task_type
        self.kwargs = kwargs
    
    def run(self):
        if self.task_type == "generate_script":
            prompt = self.kwargs.get("prompt", "")
            content = self.kwargs.get("content", "")
            result, error = GeminiAPIManager.call_gemini(prompt, content)
        elif self.task_type == "analyze_youtube":
            youtube_url = self.kwargs.get("youtube_url", "")
            prompt = self.kwargs.get("prompt", "")
            result, error = GeminiAPIManager.analyze_youtube(youtube_url, prompt)
        else:
            result, error = None, "Unknown task type"
        
        self.finished.emit(result or "", error or "")


class GeminiSettingsTab(QWidget):
    """Tab cài đặt API Key Gemini"""
    def __init__(self):
        super().__init__()
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(16)
        
        # Header
        header = QHBoxLayout()
        icon = QLabel("🔑")
        icon.setStyleSheet("font-size: 24px;")
        title = QLabel("Cài Đặt Gemini API")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #1e293b;")
        header.addWidget(icon)
        header.addWidget(title)
        header.addStretch()
        layout.addLayout(header)
        
        # Info box
        info_box = QFrame()
        info_box.setStyleSheet("""
            QFrame {
                background-color: #DBEAFE;
                border: 1px solid #93C5FD;
                border-radius: 8px;
                padding: 12px;
            }
        """)
        info_layout = QVBoxLayout(info_box)
        info_lbl = QLabel("""
            <b>🔑 Gemini API Key</b><br><br>
            API Key dùng để gọi các tính năng AI của Google Gemini:<br>
            • <b>Tab Tạo Kịch Bản:</b> Tạo kịch bản video từ câu chuyện<br>
            • <b>Tab Phân Tích YouTube:</b> Phân tích và trích xuất nội dung video<br><br>
            📌 Lấy API Key miễn phí tại: <a href="https://aistudio.google.com/apikey">https://aistudio.google.com/apikey</a>
        """)
        info_lbl.setWordWrap(True)
        info_lbl.setOpenExternalLinks(True)
        info_layout.addWidget(info_lbl)
        layout.addWidget(info_box)
        
        # API Key input
        key_group = QFrame()
        key_group.setStyleSheet("""
            QFrame {
                background-color: #F8FAFC;
                border: 1px solid #E2E8F0;
                border-radius: 8px;
                padding: 16px;
            }
        """)
        key_layout = QVBoxLayout(key_group)
        
        key_label = QLabel("API Key:")
        key_label.setStyleSheet("font-weight: 600; color: #475569;")
        key_layout.addWidget(key_label)
        
        self.api_key_input = QLineEdit()
        self.api_key_input.setPlaceholderText("AIzaSy...")
        self.api_key_input.setEchoMode(QLineEdit.Password)
        self.api_key_input.setStyleSheet("""
            QLineEdit {
                padding: 12px;
                border: 2px solid #E2E8F0;
                border-radius: 8px;
                font-size: 14px;
            }
            QLineEdit:focus {
                border-color: #6366F1;
            }
        """)
        key_layout.addWidget(self.api_key_input)
        
        # Show/Hide toggle
        self.show_key_check = QCheckBox("Hiển thị API Key")
        self.show_key_check.stateChanged.connect(self.toggle_key_visibility)
        key_layout.addWidget(self.show_key_check)
        
        layout.addWidget(key_group)
        
        # Model selection
        model_group = QFrame()
        model_group.setStyleSheet("""
            QFrame {
                background-color: #F8FAFC;
                border: 1px solid #E2E8F0;
                border-radius: 8px;
                padding: 16px;
            }
        """)
        model_layout = QVBoxLayout(model_group)
        
        model_label = QLabel("Model Gemini:")
        model_label.setStyleSheet("font-weight: 600; color: #475569;")
        model_layout.addWidget(model_label)
        
        self.model_combo = QComboBox()
        self.model_combo.addItems([
            "gemini-2.5-flash",
            "gemini-2.5-pro",
            "gemini-1.5-flash-latest",
            "gemini-1.5-pro-latest"
        ])
        self.model_combo.setStyleSheet("""
            QComboBox {
                padding: 10px;
                border: 2px solid #E2E8F0;
                border-radius: 8px;
                font-size: 14px;
            }
        """)
        model_layout.addWidget(self.model_combo)
        
        model_info = QLabel("💡 gemini-2.5-flash: Nhanh và miễn phí | gemini-2.5-pro: Chất lượng cao hơn")
        model_info.setStyleSheet("color: #64748B; font-size: 12px;")
        model_layout.addWidget(model_info)
        
        layout.addWidget(model_group)
        
        # Buttons
        btn_layout = QHBoxLayout()
        
        self.save_btn = QPushButton("💾 Lưu Cài Đặt")
        self.save_btn.setCursor(Qt.PointingHandCursor)
        self.save_btn.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #10B981, stop:1 #059669);
                color: white;
                padding: 12px 24px;
                border-radius: 8px;
                font-weight: 600;
                font-size: 14px;
                border: none;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #059669, stop:1 #047857);
            }
        """)
        self.save_btn.clicked.connect(self.save_settings)
        btn_layout.addWidget(self.save_btn)
        
        self.test_btn = QPushButton("🧪 Test API Key")
        self.test_btn.setCursor(Qt.PointingHandCursor)
        self.test_btn.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #3B82F6, stop:1 #2563EB);
                color: white;
                padding: 12px 24px;
                border-radius: 8px;
                font-weight: 600;
                font-size: 14px;
                border: none;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #2563EB, stop:1 #1D4ED8);
            }
        """)
        self.test_btn.clicked.connect(self.test_api_key)
        btn_layout.addWidget(self.test_btn)
        
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        
        # Status label
        self.status_label = QLabel("")
        self.status_label.setStyleSheet("font-size: 13px; padding: 8px;")
        layout.addWidget(self.status_label)
        
        layout.addStretch()
        self.setLayout(layout)
        
        # Load saved settings
        self.load_settings()
    
    def toggle_key_visibility(self, state):
        if state == 2:  # Checked
            self.api_key_input.setEchoMode(QLineEdit.Normal)
        else:
            self.api_key_input.setEchoMode(QLineEdit.Password)
    
    def save_settings(self):
        api_key = self.api_key_input.text().strip()
        model = self.model_combo.currentText()
        
        if not api_key:
            self.status_label.setText("⚠️ Vui lòng nhập API Key!")
            self.status_label.setStyleSheet("color: #D97706; font-size: 13px; padding: 8px;")
            return
        
        GeminiAPIManager.set_api_key(api_key)
        GeminiAPIManager.set_model(model)
        
        self.status_label.setText("✅ Đã lưu cài đặt thành công!")
        self.status_label.setStyleSheet("color: #10B981; font-size: 13px; padding: 8px;")
    
    def load_settings(self):
        # Load from GeminiAPIManager
        api_key = GeminiAPIManager.get_api_key()
        model = GeminiAPIManager.get_model()
        
        if api_key:
            self.api_key_input.setText(api_key)
        
        index = self.model_combo.findText(model)
        if index >= 0:
            self.model_combo.setCurrentIndex(index)
    
    def test_api_key(self):
        api_key = self.api_key_input.text().strip()
        if not api_key:
            self.status_label.setText("⚠️ Vui lòng nhập API Key trước!")
            self.status_label.setStyleSheet("color: #D97706; font-size: 13px; padding: 8px;")
            return
        
        self.status_label.setText("⏳ Đang kiểm tra API Key...")
        self.status_label.setStyleSheet("color: #3B82F6; font-size: 13px; padding: 8px;")
        self.test_btn.setEnabled(False)
        QApplication.processEvents()
        
        # Temporarily set the key for testing
        old_key = GeminiAPIManager.get_api_key()
        GeminiAPIManager.set_api_key(api_key)
        GeminiAPIManager.set_model(self.model_combo.currentText())
        
        result, error = GeminiAPIManager.call_gemini("Nói 'Xin chào' bằng tiếng Việt.")
        
        if error:
            self.status_label.setText(f"❌ Lỗi: {error}")
            self.status_label.setStyleSheet("color: #DC2626; font-size: 13px; padding: 8px;")
            GeminiAPIManager.set_api_key(old_key)
        else:
            self.status_label.setText(f"✅ API Key hoạt động! Phản hồi: {result[:100]}...")
            self.status_label.setStyleSheet("color: #10B981; font-size: 13px; padding: 8px;")
        
        self.test_btn.setEnabled(True)


class ScriptWritingTab(QWidget):
    """Tab tạo kịch bản với Gemini AI"""
    def __init__(self):
        super().__init__()
        self.worker = None
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(16)
        
        # Header
        header = QHBoxLayout()
        icon = QLabel("📝")
        icon.setStyleSheet("font-size: 24px;")
        title = QLabel("Tạo Kịch Bản Video AI")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #1e293b;")
        header.addWidget(icon)
        header.addWidget(title)
        header.addStretch()
        layout.addLayout(header)
        
        # Info box
        info_box = QFrame()
        info_box.setStyleSheet("""
            QFrame {
                background-color: #FEF3C7;
                border: 1px solid #FCD34D;
                border-radius: 8px;
                padding: 12px;
            }
        """)
        info_layout = QVBoxLayout(info_box)
        info_lbl = QLabel("""
            <b>📝 Tạo Kịch Bản từ Câu Chuyện</b><br>
            Nhập câu chuyện hoặc nội dung văn bản, AI sẽ tạo ra kịch bản phù hợp để làm video.<br>
            Mỗi kịch bản tương ứng với một video 8 giây.
        """)
        info_lbl.setWordWrap(True)
        info_layout.addWidget(info_lbl)
        layout.addWidget(info_box)
        
        # Input section
        input_group = QFrame()
        input_group.setStyleSheet("""
            QFrame {
                background-color: #F8FAFC;
                border: 1px solid #E2E8F0;
                border-radius: 8px;
                padding: 16px;
            }
        """)
        input_layout = QVBoxLayout(input_group)
        
        # Input method selection
        method_layout = QHBoxLayout()
        self.text_radio = QRadioButton("Nhập văn bản trực tiếp")
        self.file_radio = QRadioButton("Chọn file TXT")
        self.text_radio.setChecked(True)
        self.text_radio.toggled.connect(self.toggle_input_method)
        method_layout.addWidget(self.text_radio)
        method_layout.addWidget(self.file_radio)
        method_layout.addStretch()
        input_layout.addLayout(method_layout)
        
        # Text input
        self.input_stack = QWidget()
        stack_layout = QVBoxLayout(self.input_stack)
        stack_layout.setContentsMargins(0, 0, 0, 0)
        
        self.text_input = QTextEdit()
        self.text_input.setPlaceholderText("Nhập câu chuyện hoặc nội dung cần tạo kịch bản...\n\nVí dụ:\nMột buổi sáng mùa thu, cô gái trẻ đi dạo trong công viên. Lá vàng rơi khắp nơi, tạo nên khung cảnh thơ mộng. Cô ngồi xuống ghế đá, nhìn những đứa trẻ chơi đùa...")
        self.text_input.setMinimumHeight(150)
        stack_layout.addWidget(self.text_input)
        
        # File input (hidden by default)
        self.file_widget = QWidget()
        file_layout = QHBoxLayout(self.file_widget)
        file_layout.setContentsMargins(0, 0, 0, 0)
        self.file_path_edit = QLineEdit()
        self.file_path_edit.setPlaceholderText("Chọn file TXT...")
        self.file_path_edit.setReadOnly(True)
        file_layout.addWidget(self.file_path_edit)
        
        browse_btn = QPushButton("📂 Chọn File")
        browse_btn.clicked.connect(self.browse_file)
        file_layout.addWidget(browse_btn)
        self.file_widget.hide()
        stack_layout.addWidget(self.file_widget)
        
        input_layout.addWidget(self.input_stack)
        layout.addWidget(input_group)
        
        # Generate button
        btn_layout = QHBoxLayout()
        
        self.generate_btn = QPushButton("✨ Tạo Kịch Bản")
        self.generate_btn.setCursor(Qt.PointingHandCursor)
        self.generate_btn.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #8B5CF6, stop:1 #7C3AED);
                color: white;
                padding: 14px 28px;
                border-radius: 8px;
                font-weight: 700;
                font-size: 15px;
                border: none;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #7C3AED, stop:1 #6D28D9);
            }
            QPushButton:disabled {
                background: #94A3B8;
            }
        """)
        self.generate_btn.clicked.connect(self.generate_script)
        btn_layout.addWidget(self.generate_btn)
        
        self.copy_btn = QPushButton("📋 Copy Kết Quả")
        self.copy_btn.setCursor(Qt.PointingHandCursor)
        self.copy_btn.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #10B981, stop:1 #059669);
                color: white;
                padding: 14px 28px;
                border-radius: 8px;
                font-weight: 600;
                font-size: 14px;
                border: none;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #059669, stop:1 #047857);
            }
        """)
        self.copy_btn.clicked.connect(self.copy_result)
        btn_layout.addWidget(self.copy_btn)
        
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        
        # Output section
        output_label = QLabel("📋 Kết Quả Kịch Bản:")
        output_label.setStyleSheet("font-weight: 600; color: #475569; font-size: 14px;")
        layout.addWidget(output_label)
        
        self.output_text = QTextEdit()
        self.output_text.setReadOnly(True)
        self.output_text.setPlaceholderText("Kết quả kịch bản sẽ hiển thị ở đây...")
        self.output_text.setStyleSheet("""
            QTextEdit {
                background-color: #F8FAFC;
                border: 2px solid #E2E8F0;
                border-radius: 8px;
                padding: 12px;
                font-size: 13px;
                line-height: 1.6;
            }
        """)
        layout.addWidget(self.output_text)
        
        # Status
        self.status_label = QLabel("")
        self.status_label.setStyleSheet("color: #64748B; font-size: 12px;")
        layout.addWidget(self.status_label)
        
        self.setLayout(layout)
    
    def toggle_input_method(self, checked):
        if checked:  # Text input
            self.text_input.show()
            self.file_widget.hide()
        else:  # File input
            self.text_input.hide()
            self.file_widget.show()
    
    def browse_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Chọn file văn bản", "", "Text Files (*.txt);;All Files (*)"
        )
        if file_path:
            self.file_path_edit.setText(file_path)
    
    def generate_script(self):
        # Get input content
        if self.text_radio.isChecked():
            content = self.text_input.toPlainText().strip()
        else:
            file_path = self.file_path_edit.text().strip()
            if not file_path or not os.path.exists(file_path):
                QMessageBox.warning(self, "Lỗi", "Vui lòng chọn file TXT hợp lệ!")
                return
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
            except Exception as e:
                QMessageBox.warning(self, "Lỗi", f"Không thể đọc file: {str(e)}")
                return
        
        if not content:
            QMessageBox.warning(self, "Thiếu Nội Dung", "Vui lòng nhập nội dung hoặc chọn file!")
            return
        
        # Check API key
        if not GeminiAPIManager.get_api_key():
            QMessageBox.warning(self, "Thiếu API Key", "Vui lòng cài đặt Gemini API Key trước!\nVào tab 'Cài Đặt API' để nhập key.")
            return
        
        # Prepare prompt
        prompt = """Đọc câu chuyện sau và tạo ra một kịch bản để tạo video với AI có thể truyền tải đúng nội dung.
Chia thành các kịch bản theo thứ tự phù hợp với video 8 giây.
Mỗi kịch bản là 1 dòng.
Kết quả là văn bản thuần túy, KHÔNG thêm lời dẫn, chú thích, markdown, in đậm/in nghiêng."""
        
        # Update UI
        self.generate_btn.setEnabled(False)
        self.status_label.setText("⏳ Đang tạo kịch bản với AI...")
        self.status_label.setStyleSheet("color: #3B82F6; font-size: 12px;")
        self.output_text.setPlainText("")
        QApplication.processEvents()
        
        # Start worker thread
        self.worker = GeminiWorker("generate_script", prompt=prompt, content=content)
        self.worker.finished.connect(self.on_script_generated)
        self.worker.start()
    
    def on_script_generated(self, result, error):
        self.generate_btn.setEnabled(True)
        
        if error:
            self.status_label.setText(f"❌ Lỗi: {error}")
            self.status_label.setStyleSheet("color: #DC2626; font-size: 12px;")
            QMessageBox.warning(self, "Lỗi", error)
        else:
            self.output_text.setPlainText(result)
            lines = len(result.strip().split('\n'))
            self.status_label.setText(f"✅ Đã tạo {lines} kịch bản thành công!")
            self.status_label.setStyleSheet("color: #10B981; font-size: 12px;")
    
    def copy_result(self):
        text = self.output_text.toPlainText()
        if text:
            QApplication.clipboard().setText(text)
            self.status_label.setText("📋 Đã copy vào clipboard!")
            self.status_label.setStyleSheet("color: #10B981; font-size: 12px;")
        else:
            QMessageBox.information(self, "Thông báo", "Chưa có kết quả để copy!")


class YouTubeAnalysisTab(QWidget):
    """Tab phân tích video YouTube với Gemini"""
    def __init__(self):
        super().__init__()
        self.worker = None
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(16)
        
        # Header
        header = QHBoxLayout()
        icon = QLabel("📺")
        icon.setStyleSheet("font-size: 24px;")
        title = QLabel("Phân Tích Video YouTube")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #1e293b;")
        header.addWidget(icon)
        header.addWidget(title)
        header.addStretch()
        layout.addLayout(header)
        
        # Info box
        info_box = QFrame()
        info_box.setStyleSheet("""
            QFrame {
                background-color: #DCFCE7;
                border: 1px solid #86EFAC;
                border-radius: 8px;
                padding: 12px;
            }
        """)
        info_layout = QVBoxLayout(info_box)
        info_lbl = QLabel("""
            <b>📺 Phân Tích Video YouTube với AI</b><br>
            Nhập link YouTube, Gemini sẽ xem video và tạo kịch bản tương ứng để bạn tái tạo video bằng AI.<br>
            Chỉ cần paste link là có thể lấy kịch bản ngay!
        """)
        info_lbl.setWordWrap(True)
        info_layout.addWidget(info_lbl)
        layout.addWidget(info_box)
        
        # URL Input
        url_group = QFrame()
        url_group.setStyleSheet("""
            QFrame {
                background-color: #F8FAFC;
                border: 1px solid #E2E8F0;
                border-radius: 8px;
                padding: 16px;
            }
        """)
        url_layout = QVBoxLayout(url_group)
        
        url_label = QLabel("🔗 Link YouTube:")
        url_label.setStyleSheet("font-weight: 600; color: #475569;")
        url_layout.addWidget(url_label)
        
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("https://www.youtube.com/watch?v=... hoặc https://youtu.be/...")
        self.url_input.setStyleSheet("""
            QLineEdit {
                padding: 12px;
                border: 2px solid #E2E8F0;
                border-radius: 8px;
                font-size: 14px;
            }
            QLineEdit:focus {
                border-color: #EF4444;
            }
        """)
        url_layout.addWidget(self.url_input)
        
        # Custom prompt (optional)
        prompt_label = QLabel("📝 Hướng dẫn tùy chỉnh (tùy chọn):")
        prompt_label.setStyleSheet("font-weight: 600; color: #475569; margin-top: 12px;")
        url_layout.addWidget(prompt_label)
        
        self.custom_prompt = QTextEdit()
        self.custom_prompt.setPlaceholderText("Để trống để dùng hướng dẫn mặc định, hoặc nhập hướng dẫn riêng của bạn...\n\nVí dụ: 'Tóm tắt nội dung video này thành 5 điểm chính'")
        self.custom_prompt.setMaximumHeight(80)
        url_layout.addWidget(self.custom_prompt)
        
        layout.addWidget(url_group)
        
        # Buttons
        btn_layout = QHBoxLayout()
        
        self.analyze_btn = QPushButton("🔍 Phân Tích Video")
        self.analyze_btn.setCursor(Qt.PointingHandCursor)
        self.analyze_btn.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #EF4444, stop:1 #DC2626);
                color: white;
                padding: 14px 28px;
                border-radius: 8px;
                font-weight: 700;
                font-size: 15px;
                border: none;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #DC2626, stop:1 #B91C1C);
            }
            QPushButton:disabled {
                background: #94A3B8;
            }
        """)
        self.analyze_btn.clicked.connect(self.analyze_video)
        btn_layout.addWidget(self.analyze_btn)
        
        self.copy_btn = QPushButton("📋 Copy Kết Quả")
        self.copy_btn.setCursor(Qt.PointingHandCursor)
        self.copy_btn.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #10B981, stop:1 #059669);
                color: white;
                padding: 14px 28px;
                border-radius: 8px;
                font-weight: 600;
                font-size: 14px;
                border: none;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #059669, stop:1 #047857);
            }
        """)
        self.copy_btn.clicked.connect(self.copy_result)
        btn_layout.addWidget(self.copy_btn)
        
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        
        # Output section
        output_label = QLabel("📋 Kết Quả Phân Tích:")
        output_label.setStyleSheet("font-weight: 600; color: #475569; font-size: 14px;")
        layout.addWidget(output_label)
        
        self.output_text = QTextEdit()
        self.output_text.setReadOnly(True)
        self.output_text.setPlaceholderText("Kết quả phân tích sẽ hiển thị ở đây...")
        self.output_text.setStyleSheet("""
            QTextEdit {
                background-color: #F8FAFC;
                border: 2px solid #E2E8F0;
                border-radius: 8px;
                padding: 12px;
                font-size: 13px;
                line-height: 1.6;
            }
        """)
        layout.addWidget(self.output_text)
        
        # Status
        self.status_label = QLabel("")
        self.status_label.setStyleSheet("color: #64748B; font-size: 12px;")
        layout.addWidget(self.status_label)
        
        self.setLayout(layout)
    
    def validate_youtube_url(self, url):
        """Validate YouTube URL format"""
        # Patterns hỗ trợ nhiều định dạng YouTube URL hơn
        patterns = [
            r'(https?://)?(www\.)?youtube\.com/watch\?v=[\w_-]+',  # Standard watch URL
            r'(https?://)?(www\.)?youtube\.com/watch\?.*v=[\w_-]+',  # Watch URL với params khác
            r'(https?://)?(www\.)?youtu\.be/[\w_-]+',  # Short URL
            r'(https?://)?(www\.)?youtube\.com/shorts/[\w_-]+',  # Shorts
            r'(https?://)?(www\.)?youtube\.com/embed/[\w_-]+',  # Embed URL
            r'(https?://)?(www\.)?youtube\.com/v/[\w_-]+',  # Old embed format
        ]
        for pattern in patterns:
            if re.match(pattern, url):
                return True
        return False
    
    def analyze_video(self):
        url = self.url_input.text().strip()
        
        if not url:
            QMessageBox.warning(self, "Thiếu Link", "Vui lòng nhập link video YouTube!")
            return
        
        if not self.validate_youtube_url(url):
            QMessageBox.warning(self, "Link Không Hợp Lệ", "Link YouTube không đúng định dạng!\n\nVí dụ hợp lệ:\n- https://www.youtube.com/watch?v=xxxxx\n- https://youtu.be/xxxxx")
            return
        
        # Check API key
        if not GeminiAPIManager.get_api_key():
            QMessageBox.warning(self, "Thiếu API Key", "Vui lòng cài đặt Gemini API Key trước!\nVào tab 'Cài Đặt API' để nhập key.")
            return
        
        # Get custom prompt if provided
        custom_prompt = self.custom_prompt.toPlainText().strip()
        
        # Update UI
        self.analyze_btn.setEnabled(False)
        self.status_label.setText("⏳ Đang phân tích video... (có thể mất 1-2 phút)")
        self.status_label.setStyleSheet("color: #3B82F6; font-size: 12px;")
        self.output_text.setPlainText("")
        QApplication.processEvents()
        
        # Start worker thread
        self.worker = GeminiWorker("analyze_youtube", youtube_url=url, prompt=custom_prompt)
        self.worker.finished.connect(self.on_analysis_complete)
        self.worker.start()
    
    def on_analysis_complete(self, result, error):
        self.analyze_btn.setEnabled(True)
        
        if error:
            self.status_label.setText(f"❌ Lỗi: {error}")
            self.status_label.setStyleSheet("color: #DC2626; font-size: 12px;")
            QMessageBox.warning(self, "Lỗi", error)
        else:
            self.output_text.setPlainText(result)
            lines = len(result.strip().split('\n'))
            self.status_label.setText(f"✅ Phân tích hoàn tất! Đã tạo {lines} kịch bản.")
            self.status_label.setStyleSheet("color: #10B981; font-size: 12px;")
    
    def copy_result(self):
        text = self.output_text.toPlainText()
        if text:
            QApplication.clipboard().setText(text)
            self.status_label.setText("📋 Đã copy vào clipboard!")
            self.status_label.setStyleSheet("color: #10B981; font-size: 12px;")
        else:
            QMessageBox.information(self, "Thông báo", "Chưa có kết quả để copy!")


class SuperSyncTab(QWidget):
    def __init__(self):
        super().__init__()
        layout = QVBoxLayout()
        label = QLabel("Chức năng Siêu Đồng Bộ (Đang phát triển)")
        label.setAlignment(Qt.AlignCenter)
        layout.addWidget(label)
        self.setLayout(layout)

class CookieDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Cài Đặt Cookies")
        self.resize(600, 400)
        layout = QVBoxLayout()
        
        info_box = QFrame()
        info_box.setStyleSheet("background-color: #FFF3CD; border: 1px solid #FFEeba; border-radius: 5px; padding: 10px;")
        info_layout = QVBoxLayout(info_box)
        info_lbl = QLabel("💡 <b>Giới hạn mỗi cookie:</b><br>• Text/Image to Video: 5 tasks đồng thời<br>• Với Upscale: 3 tasks đồng thời<br><br>VD: 2 Acc = 10 tasks cùng lúc.")
        info_lbl.setWordWrap(True)
        info_layout.addWidget(info_lbl)
        layout.addWidget(info_box)
        
        layout.addWidget(QLabel("Nhập cookies (tối đa 1 cookie, 1 cookie/1 dòng):"))
        self.text_edit = QTextEdit()
        self.text_edit.setPlaceholderText("Cookie 1...")
        layout.addWidget(self.text_edit)
        
        btn_box = QHBoxLayout()
        btn_box.addStretch()
        self.ok_btn = QPushButton("OK")
        self.ok_btn.clicked.connect(self.accept)
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.clicked.connect(self.reject)
        btn_box.addWidget(self.ok_btn)
        btn_box.addWidget(self.cancel_btn)
        layout.addLayout(btn_box)
        self.setLayout(layout)

class ApiKeyDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Cài Đặt Gemini API Keys")
        self.resize(600, 300)
        layout = QVBoxLayout()
        
        info_box = QFrame()
        info_box.setStyleSheet("background-color: #D1ECF1; border: 1px solid #BEE5EB; border-radius: 5px; padding: 10px;")
        info_layout = QVBoxLayout(info_box)
        info_lbl = QLabel("🔑 <b>Gemini API Keys - Dùng chung cho toàn app:</b><br>• Tab Siêu Đồng Bộ: Phân tích và tạo prompt<br>• Tab Viết Kịch Bản: Tạo prompt video tự động")
        info_lbl.setWordWrap(True)
        info_layout.addWidget(info_lbl)
        layout.addWidget(info_box)
        
        layout.addWidget(QLabel("Nhập Gemini API Keys (1 key/1 dòng):"))
        self.text_edit = QTextEdit()
        self.text_edit.setPlaceholderText("AIzaSy...")
        layout.addWidget(self.text_edit)
        
        btn_box = QHBoxLayout()
        btn_box.addStretch()
        self.ok_btn = QPushButton("OK")
        self.ok_btn.clicked.connect(self.accept)
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.clicked.connect(self.reject)
        btn_box.addWidget(self.ok_btn)
        btn_box.addWidget(self.cancel_btn)
        layout.addLayout(btn_box)
        self.setLayout(layout)

class AccountManager(QMainWindow):
    def __init__(self):
        super().__init__()
        self.accounts = []
        self.worker_threads = []
        self.account_workers = {}
        self.enqueued_task_ids = set()
        self.max_tasks_normal = 5
        self.max_tasks_1080 = 3
        self.running_counts = {}
        self.global_max_concurrent_tasks = 5
        self.total_running_tasks = 0
        self.prompt_delay = 0.5
        self.task_timeout_seconds = 30
        self.running_tasks_by_account = {}
        self.session_video_hashes = set()
        self.session_image_hashes = set()
        self._is_start_processing = False
        self._saved_main_sizes = None

        self.task_start_times = {}
        self.task_complete_times = {}
        self.account_ref_success = {}
        self.task_finished_by_manager = set()
        self.blocked_accounts = set()
        self.timeout_warned_tasks = set()
        self.loading_config = False

        self.timeout_timer = QTimer()
        self.timeout_timer.setInterval(1000)
        self.timeout_timer.timeout.connect(self.check_task_timeouts)
        self.timeout_timer.start()

        app_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        self.output_folder = os.path.join(app_dir, "output")
        self.config_file = os.path.join(app_dir, "config.json")

        # mảng thư mục media (chỉ hình ảnh / video)
        self.media_subfolders = ["Images", "Videos"]
        # thư mục lưu prompt đưa ra ngoài output (cùng level với config)
        self.prompt_folder = os.path.join(app_dir, "Prompts")

        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder, exist_ok=True)
        for name in self.media_subfolders:
            os.makedirs(os.path.join(self.output_folder, name), exist_ok=True)
        os.makedirs(self.prompt_folder, exist_ok=True)

        self.init_ui()
        self.loading_config = True
        self.load_config()
        self.loading_config = False

    def init_ui(self):
        self.setWindowTitle("AUto VEO3 by Fath - Professional Video & Image Generator")
        self.resize(1700, 950)
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # ========== HEADER BAR ==========
        header_widget = QWidget()
        header_widget.setObjectName("headerBar")
        header_widget.setFixedHeight(70)
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(20, 10, 20, 10)
        
        # Logo and Title
        logo_title_layout = QHBoxLayout()
        logo_label = QLabel("🎬")
        logo_label.setStyleSheet("font-size: 32px;")
        logo_title_layout.addWidget(logo_label)
        
        title_container = QVBoxLayout()
        title_container.setSpacing(2)
        app_title = QLabel("AUto VEO3 by Fath")
        app_title.setStyleSheet("""
            font-size: 22px;
            font-weight: bold;
            color: #ffffff;
            letter-spacing: 1px;
        """)
        app_subtitle = QLabel("Professional AI Video & Image Generator")
        app_subtitle.setStyleSheet("""
            font-size: 11px;
            color: rgba(255, 255, 255, 0.7);
        """)
        title_container.addWidget(app_title)
        title_container.addWidget(app_subtitle)
        logo_title_layout.addLayout(title_container)
        logo_title_layout.addStretch()
        header_layout.addLayout(logo_title_layout)
        
        # Status indicators in header
        status_layout = QHBoxLayout()
        status_layout.setSpacing(15)
        
        self.status_indicator = QLabel("● Ready")
        self.status_indicator.setStyleSheet("""
            color: #10B981;
            font-size: 12px;
            font-weight: 600;
        """)
        status_layout.addWidget(self.status_indicator)
        
        header_layout.addLayout(status_layout)
        main_layout.addWidget(header_widget)
        
        # ========== MAIN CONTENT AREA ==========
        content_widget = QWidget()
        content_layout = QHBoxLayout(content_widget)
        content_layout.setContentsMargins(15, 15, 15, 15)
        content_layout.setSpacing(15)
        
        self.main_splitter = QSplitter(Qt.Horizontal)
        self.main_splitter.setHandleWidth(3)
        
        left_panel = self.create_left_panel()
        self.main_splitter.addWidget(left_panel)
        
        right_panel = self.create_right_panel()
        self.main_splitter.addWidget(right_panel)
        
        self.main_splitter.setStretchFactor(0, 4)
        self.main_splitter.setStretchFactor(1, 6)

        content_layout.addWidget(self.main_splitter)
        main_layout.addWidget(content_widget)
        
        central_widget.setLayout(main_layout)
        self.apply_styles()

        self.result_table.request_regenerate.connect(self.handle_regenerate_task)
        self.result_table.request_run_image.connect(self.handle_run_single_task)
        self.result_table.request_run_video.connect(self.handle_run_single_task)
        
        self.image_tab.add_queue_btn.clicked.connect(self.add_image_task_to_queue)
        self.video_tab.add_queue_btn.clicked.connect(self.add_video_task_to_queue)
        self.tab_widget.currentChanged.connect(self.on_tab_changed)
        self.result_table.set_mode("image")

        self.thread_spin.valueChanged.connect(self.on_thread_spin_changed)
        self.prompt_delay_spin.valueChanged.connect(self.on_prompt_delay_changed)
        self.direct_project_check.stateChanged.connect(self.on_direct_project_changed)
        self.hide_browser_check.stateChanged.connect(self.on_hide_browser_changed)
        self.upscale_check.stateChanged.connect(self.on_upscale_changed)
        self.video_tab.mode_changed.connect(self.on_video_mode_changed)

    def create_left_panel(self):
        # Scroll Area bao quanh toàn bộ Panel Trái
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll_area.setObjectName("leftScrollArea")
        
        container = QWidget()
        container.setObjectName("leftContainer")
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)
        
        # --- 1. TAB CHỨC NĂNG (ĐƯỢC ĐƯA VÀO PANEL TRÁI) ---
        self.tab_widget = QTabWidget()
        self.tab_widget.setObjectName("mainTabWidget")
        self.tab_widget.setDocumentMode(True)
        
        self.image_tab = ImageGenerationTab()
        self.tab_widget.addTab(self.image_tab, "🖼️ Tạo Ảnh")
        
        self.video_tab = VideoGenerationTab()
        self.tab_widget.addTab(self.video_tab, "🎬 Tạo Video")
        
        self.account_tab = AccountTab()
        self.tab_widget.addTab(self.account_tab, "👤 Tài Khoản")
        
        self.script_tab = ScriptWritingTab()
        self.tab_widget.addTab(self.script_tab, "📝 Kịch Bản")
        
        self.youtube_tab = YouTubeAnalysisTab()
        self.tab_widget.addTab(self.youtube_tab, "📺 YouTube")
        
        self.gemini_settings_tab = GeminiSettingsTab()
        self.tab_widget.addTab(self.gemini_settings_tab, "🔑 API")
        
        self.sync_tab = SuperSyncTab()
        self.tab_widget.addTab(self.sync_tab, "🔄 Đồng Bộ")
        
        layout.addWidget(self.tab_widget)
        
        # --- 2. PHẦN CÀI ĐẶT & BUTTON (Modern Card Design) ---
        self.control_group = QFrame()
        self.control_group.setObjectName("controlCard")
        control_layout = QVBoxLayout()
        control_layout.setContentsMargins(16, 16, 16, 16)
        control_layout.setSpacing(12)
        
        # Header with icon
        settings_header = QHBoxLayout()
        settings_icon = QLabel("⚙️")
        settings_icon.setStyleSheet("font-size: 18px;")
        settings_label = QLabel("Cài đặt Chung")
        settings_label.setObjectName("sectionTitle")
        settings_header.addWidget(settings_icon)
        settings_header.addWidget(settings_label)
        settings_header.addStretch()
        control_layout.addLayout(settings_header)
        
        # Divider
        divider = QFrame()
        divider.setObjectName("divider")
        divider.setFixedHeight(1)
        control_layout.addWidget(divider)
        
        # Settings Grid
        settings_grid = QGridLayout()
        settings_grid.setSpacing(10)
        
        # Thread count
        thread_label = QLabel("🔄 Số luồng:")
        thread_label.setObjectName("settingLabel")
        settings_grid.addWidget(thread_label, 0, 0)
        self.thread_spin = QSpinBox()
        self.thread_spin.setObjectName("modernSpinBox")
        self.thread_spin.setMinimum(1)
        self.thread_spin.setMaximum(20)
        self.thread_spin.setValue(5)
        settings_grid.addWidget(self.thread_spin, 0, 1)

        # Delay
        delay_label = QLabel("⏱️ Delay (giây):")
        delay_label.setObjectName("settingLabel")
        settings_grid.addWidget(delay_label, 1, 0)
        self.prompt_delay_spin = QDoubleSpinBox()
        self.prompt_delay_spin.setObjectName("modernSpinBox")
        self.prompt_delay_spin.setDecimals(1)
        self.prompt_delay_spin.setSingleStep(0.1)
        self.prompt_delay_spin.setMinimum(0.0)
        self.prompt_delay_spin.setMaximum(60.0)
        self.prompt_delay_spin.setValue(0.5)
        settings_grid.addWidget(self.prompt_delay_spin, 1, 1)
        
        control_layout.addLayout(settings_grid)

        # Output folder
        output_layout = QHBoxLayout()
        output_label = QLabel("📁 Thư mục lưu:")
        output_label.setObjectName("settingLabel")
        output_layout.addWidget(output_label)
        self.output_path = QLineEdit()
        self.output_path.setObjectName("modernLineEdit")
        self.output_path.setText(self.output_folder)
        self.output_path.setPlaceholderText("Chọn thư mục lưu kết quả...")
        output_layout.addWidget(self.output_path)
        
        browse_output_btn = QPushButton("📂")
        browse_output_btn.setObjectName("iconButton")
        browse_output_btn.setToolTip("Chọn thư mục")
        browse_output_btn.setMaximumWidth(40)
        browse_output_btn.clicked.connect(self.browse_output_folder)
        output_layout.addWidget(browse_output_btn)
        control_layout.addLayout(output_layout)
        
        # Checkbox options
        options_layout = QVBoxLayout()
        options_layout.setSpacing(8)
        
        self.upscale_check = QCheckBox("🔮 Upscale 1080p (Video)")
        self.upscale_check.setObjectName("modernCheckbox")
        options_layout.addWidget(self.upscale_check)
        
        self.direct_project_check = QCheckBox("🚀 Vào thẳng dự án")
        self.direct_project_check.setObjectName("modernCheckbox")
        options_layout.addWidget(self.direct_project_check)

        self.hide_browser_check = QCheckBox("👁️ Ẩn cửa sổ Chrome (chạy ngầm)")
        self.hide_browser_check.setObjectName("modernCheckbox")
        options_layout.addWidget(self.hide_browser_check)
        
        control_layout.addLayout(options_layout)
        
        # Divider before buttons
        divider2 = QFrame()
        divider2.setObjectName("divider")
        divider2.setFixedHeight(1)
        control_layout.addWidget(divider2)
                
        # Action Buttons
        btn_grid = QGridLayout()
        btn_grid.setSpacing(10)
        
        self.start_btn = QPushButton("▶️ BẮT ĐẦU")
        self.start_btn.setObjectName("primaryButton")
        self.start_btn.setCursor(Qt.PointingHandCursor)
        self.start_btn.clicked.connect(self.start_processing)
        btn_grid.addWidget(self.start_btn, 0, 0)
        
        self.stop_btn = QPushButton("⏹️ DỪNG LẠI")
        self.stop_btn.setObjectName("dangerButton")
        self.stop_btn.setCursor(Qt.PointingHandCursor)
        self.stop_btn.clicked.connect(self.stop_processing)
        self.stop_btn.setEnabled(False)
        btn_grid.addWidget(self.stop_btn, 0, 1)
        
        open_folder_btn = QPushButton("📂 Mở Thư Mục")
        open_folder_btn.setObjectName("secondaryButton")
        open_folder_btn.setCursor(Qt.PointingHandCursor)
        open_folder_btn.clicked.connect(self.open_output_folder)
        btn_grid.addWidget(open_folder_btn, 1, 0)
        
        save_config_btn = QPushButton("💾 Lưu Cấu Hình")
        save_config_btn.setObjectName("warningButton")
        save_config_btn.setCursor(Qt.PointingHandCursor)
        save_config_btn.clicked.connect(self.save_config)
        btn_grid.addWidget(save_config_btn, 1, 1)
        
        control_layout.addLayout(btn_grid)
        
        # Quick Settings Section
        quick_header = QHBoxLayout()
        quick_icon = QLabel("⚡")
        quick_icon.setStyleSheet("font-size: 16px;")
        quick_label = QLabel("Cài đặt nhanh")
        quick_label.setObjectName("subSectionTitle")
        quick_header.addWidget(quick_icon)
        quick_header.addWidget(quick_label)
        quick_header.addStretch()
        control_layout.addLayout(quick_header)
        
        quick_btn_layout = QHBoxLayout()
        quick_btn_layout.setSpacing(10)
        
        self.quick_cookie_btn = QPushButton("🍪 Thêm Cookie")
        self.quick_cookie_btn.setObjectName("outlineButton")
        self.quick_cookie_btn.setCursor(Qt.PointingHandCursor)
        self.quick_cookie_btn.clicked.connect(self.open_cookie_dialog)
        quick_btn_layout.addWidget(self.quick_cookie_btn)
        
        self.quick_api_btn = QPushButton("🔑 Cài API Key")
        self.quick_api_btn.setObjectName("outlineButton")
        self.quick_api_btn.setCursor(Qt.PointingHandCursor)
        self.quick_api_btn.clicked.connect(self.open_api_key_dialog)
        quick_btn_layout.addWidget(self.quick_api_btn)
        
        control_layout.addLayout(quick_btn_layout)
        
        self.control_group.setLayout(control_layout)
        layout.addWidget(self.control_group)
        
        layout.addStretch()
        
        container.setLayout(layout)
        scroll_area.setWidget(container)
        
        return scroll_area

    def on_tab_changed(self, index):
        if index == 0:
            self.result_table.set_mode("image")
        elif index == 1:
            self.result_table.set_mode("video")

        # Tabs không cần hiển thị panel phải: Tài Khoản (2), Kịch Bản (3), YouTube (4), API (5), Đồng Bộ (6)
        hide_right_panel_tabs = [2, 3, 4, 5, 6]
        
        if index in hide_right_panel_tabs:
            if self.main_splitter.widget(1).isVisible():
                if not self._saved_main_sizes:
                    self._saved_main_sizes = self.main_splitter.sizes()
            self.main_splitter.widget(1).hide()
            self.main_splitter.setSizes([1, 0])
            if hasattr(self, "control_group"):
                self.control_group.hide()
        else:
            self.main_splitter.widget(1).show()
            if self._saved_main_sizes:
                self.main_splitter.setSizes(self._saved_main_sizes)
            if hasattr(self, "control_group"):
                self.control_group.show()

    # --- HÀM XỬ LÝ NÚT CÀI ĐẶT NHANH ---
    def open_cookie_dialog(self):
        dialog = CookieDialog(self)
        if dialog.exec():
            cookies = dialog.text_edit.toPlainText()
            if cookies:
                self.account_tab.cookie_text.setText(cookies)
                # Tự động kích hoạt nút thêm nếu cần
                self.account_tab.add_account() 
                self.log_widget.add_log("Đã cập nhật Cookie từ cài đặt nhanh.", "success")

    def open_api_key_dialog(self):
        dialog = ApiKeyDialog(self)
        # Pre-fill with existing key if available
        existing_key = GeminiAPIManager.get_api_key()
        if existing_key:
            dialog.text_edit.setPlainText(existing_key)
        
        if dialog.exec():
            keys = dialog.text_edit.toPlainText().strip()
            if keys:
                # Get the first key (one key per line)
                first_key = keys.splitlines()[0].strip()
                GeminiAPIManager.set_api_key(first_key)
                # Update the settings tab if it exists
                if hasattr(self, 'gemini_settings_tab'):
                    self.gemini_settings_tab.api_key_input.setText(first_key)
                self.log_widget.add_log("Đã lưu Gemini API Key.", "success")
 
    def create_right_panel(self):
        widget = QWidget()
        widget.setObjectName("rightPanel")
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        # Header with icon
        header_layout = QHBoxLayout()
        header_icon = QLabel("📊")
        header_icon.setStyleSheet("font-size: 18px;")
        title_label = QLabel("Bảng Tiến Trình")
        title_label.setObjectName("panelTitle")
        header_layout.addWidget(header_icon)
        header_layout.addWidget(title_label)
        header_layout.addStretch()
        
        # Stats badges
        self.task_count_badge = QLabel("0 tác vụ")
        self.task_count_badge.setObjectName("statsBadge")
        header_layout.addWidget(self.task_count_badge)
        
        layout.addLayout(header_layout)

        self.result_table = ResultTable()
        self.log_widget = LogWidget()
        
        bottom_info_widget = QWidget()
        bottom_info_widget.setObjectName("logContainer")
        bottom_info_layout = QVBoxLayout()
        bottom_info_layout.setContentsMargins(0, 0, 0, 0)
        bottom_info_layout.addWidget(self.log_widget)
        bottom_info_widget.setLayout(bottom_info_layout)
        
        # Dùng Splitter Dọc để chia Bảng (Trên) và Log (Dưới)
        self.right_splitter = QSplitter(Qt.Vertical)
        self.right_splitter.setObjectName("rightSplitter")
        self.right_splitter.addWidget(self.result_table)
        self.right_splitter.addWidget(bottom_info_widget)
        
        self.right_splitter.setStretchFactor(0, 7)
        self.right_splitter.setStretchFactor(1, 3)
        
        layout.addWidget(self.right_splitter)

        widget.setLayout(layout)
        
        return widget
        
    def apply_styles(self):
        self.setStyleSheet("""
            /* ============================================
               AUto VEO3 by Fath - Modern UI Theme
               Professional Dark/Light Hybrid Design
            ============================================ */
            
            QMainWindow {
                background-color: #f8fafc;
            }
            
            /* ========== HEADER BAR ========== */
            #headerBar {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 #1e293b, stop:0.5 #334155, stop:1 #475569);
                border-bottom: 3px solid #6366f1;
            }
            
            /* ========== LEFT CONTAINER ========== */
            #leftScrollArea {
                background-color: transparent;
                border: none;
            }
            #leftContainer {
                background-color: transparent;
            }
            
            /* ========== MODERN TABS ========== */
            #mainTabWidget::pane {
                background-color: #ffffff;
                border: 1px solid #e2e8f0;
                border-radius: 12px;
                margin-top: -1px;
            }
            #mainTabWidget > QTabBar::tab {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #f1f5f9, stop:1 #e2e8f0);
                color: #64748b;
                padding: 12px 24px;
                margin-right: 4px;
                border-top-left-radius: 10px;
                border-top-right-radius: 10px;
                border: 1px solid #e2e8f0;
                border-bottom: none;
                font-weight: 600;
                font-size: 13px;
            }
            #mainTabWidget > QTabBar::tab:selected {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #ffffff, stop:1 #f8fafc);
                color: #6366f1;
                border-color: #6366f1;
                border-bottom: 3px solid #ffffff;
            }
            #mainTabWidget > QTabBar::tab:hover:!selected {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #eef2ff, stop:1 #e0e7ff);
                color: #4f46e5;
            }
            
            /* ========== CONTROL CARD ========== */
            #controlCard {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #ffffff, stop:1 #f8fafc);
                border: 1px solid #e2e8f0;
                border-radius: 16px;
                margin-top: 12px;
            }
            
            #sectionTitle {
                font-size: 16px;
                font-weight: 700;
                color: #1e293b;
                letter-spacing: 0.5px;
            }
            
            #subSectionTitle {
                font-size: 13px;
                font-weight: 600;
                color: #475569;
            }
            
            #divider {
                background-color: #e2e8f0;
                border: none;
            }
            
            #settingLabel {
                color: #64748b;
                font-size: 13px;
                font-weight: 500;
            }
            
            /* ========== MODERN INPUTS ========== */
            #modernSpinBox, #modernLineEdit {
                background-color: #f8fafc;
                border: 2px solid #e2e8f0;
                border-radius: 8px;
                padding: 8px 12px;
                color: #1e293b;
                font-size: 13px;
            }
            #modernSpinBox:focus, #modernLineEdit:focus {
                border-color: #6366f1;
                background-color: #ffffff;
            }
            #modernSpinBox:hover, #modernLineEdit:hover {
                border-color: #a5b4fc;
            }
            
            QLineEdit, QTextEdit, QSpinBox, QDoubleSpinBox {
                background-color: #f8fafc;
                border: 2px solid #e2e8f0;
                border-radius: 8px;
                padding: 10px 14px;
                color: #1e293b;
                font-size: 13px;
                selection-background-color: #6366f1;
                selection-color: white;
            }
            QLineEdit:focus, QTextEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus {
                border-color: #6366f1;
                background-color: #ffffff;
            }
            
            QComboBox {
                background-color: #f8fafc;
                border: 2px solid #e2e8f0;
                border-radius: 8px;
                padding: 10px 14px;
                color: #1e293b;
                font-size: 13px;
                min-height: 22px;
            }
            QComboBox:hover {
                border-color: #a5b4fc;
            }
            QComboBox:focus {
                border-color: #6366f1;
            }
            QComboBox::drop-down {
                border: none;
                width: 30px;
                subcontrol-position: right center;
            }
            QComboBox QAbstractItemView {
                background-color: #ffffff;
                border: 2px solid #e2e8f0;
                border-radius: 8px;
                color: #1e293b;
                selection-background-color: #6366f1;
                selection-color: white;
                padding: 6px;
            }
            
            /* ========== MODERN CHECKBOXES ========== */
            #modernCheckbox {
                color: #475569;
                font-size: 13px;
                font-weight: 500;
                spacing: 8px;
            }
            #modernCheckbox::indicator {
                width: 20px;
                height: 20px;
                border-radius: 6px;
                border: 2px solid #cbd5e1;
                background-color: #f8fafc;
            }
            #modernCheckbox::indicator:checked {
                background-color: #6366f1;
                border-color: #6366f1;
            }
            #modernCheckbox::indicator:hover {
                border-color: #6366f1;
            }
            QCheckBox {
                color: #475569;
                font-size: 13px;
                spacing: 8px;
            }
            QCheckBox::indicator {
                width: 18px;
                height: 18px;
                border-radius: 5px;
                border: 2px solid #cbd5e1;
                background-color: #f8fafc;
            }
            QCheckBox::indicator:checked {
                background-color: #6366f1;
                border-color: #6366f1;
            }
            
            /* ========== ACTION BUTTONS ========== */
            #primaryButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #22c55e, stop:1 #16a34a);
                color: white;
                border: none;
                border-radius: 10px;
                padding: 14px 24px;
                font-size: 14px;
                font-weight: 700;
                letter-spacing: 1px;
            }
            #primaryButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #16a34a, stop:1 #15803d);
            }
            #primaryButton:pressed {
                background-color: #166534;
            }
            
            #dangerButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #ef4444, stop:1 #dc2626);
                color: white;
                border: none;
                border-radius: 10px;
                padding: 14px 24px;
                font-size: 14px;
                font-weight: 700;
                letter-spacing: 1px;
            }
            #dangerButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #dc2626, stop:1 #b91c1c);
            }
            #dangerButton:disabled {
                background: #fecaca;
                color: #fca5a5;
            }
            
            #secondaryButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #3b82f6, stop:1 #2563eb);
                color: white;
                border: none;
                border-radius: 10px;
                padding: 12px 20px;
                font-size: 13px;
                font-weight: 600;
            }
            #secondaryButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #2563eb, stop:1 #1d4ed8);
            }
            
            #warningButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #f59e0b, stop:1 #d97706);
                color: white;
                border: none;
                border-radius: 10px;
                padding: 12px 20px;
                font-size: 13px;
                font-weight: 600;
            }
            #warningButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #d97706, stop:1 #b45309);
            }
            
            #outlineButton {
                background-color: transparent;
                color: #6366f1;
                border: 2px solid #6366f1;
                border-radius: 8px;
                padding: 10px 16px;
                font-size: 12px;
                font-weight: 600;
            }
            #outlineButton:hover {
                background-color: #eef2ff;
                color: #4f46e5;
            }
            
            #iconButton {
                background-color: #f1f5f9;
                border: 2px solid #e2e8f0;
                border-radius: 8px;
                padding: 8px;
                font-size: 14px;
            }
            #iconButton:hover {
                background-color: #e0e7ff;
                border-color: #6366f1;
            }
            
            /* Generic button fallback */
            QPushButton {
                background-color: #f1f5f9;
                color: #475569;
                border: 2px solid #e2e8f0;
                border-radius: 8px;
                padding: 10px 18px;
                font-size: 13px;
                font-weight: 600;
            }
            QPushButton:hover {
                background-color: #e0e7ff;
                border-color: #6366f1;
                color: #4f46e5;
            }
            QPushButton:pressed {
                background-color: #c7d2fe;
            }
            QPushButton:disabled {
                background-color: #f1f5f9;
                color: #94a3b8;
                border-color: #e2e8f0;
            }
            
            /* ========== RIGHT PANEL ========== */
            #rightPanel {
                background-color: transparent;
            }
            
            #panelTitle {
                font-size: 16px;
                font-weight: 700;
                color: #1e293b;
            }
            
            #statsBadge {
                background-color: #eef2ff;
                color: #6366f1;
                padding: 6px 12px;
                border-radius: 20px;
                font-size: 12px;
                font-weight: 600;
            }
            
            /* ========== MODERN TABLE ========== */
            QTableWidget {
                background-color: #ffffff;
                border: 1px solid #e2e8f0;
                border-radius: 12px;
                gridline-color: #f1f5f9;
                selection-background-color: #eef2ff;
                selection-color: #1e293b;
                font-size: 13px;
            }
            QTableWidget::item {
                padding: 8px;
                border-bottom: 1px solid #f1f5f9;
            }
            QTableWidget::item:selected {
                background-color: #eef2ff;
            }
            QHeaderView::section {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #f8fafc, stop:1 #f1f5f9);
                padding: 12px 8px;
                border: none;
                border-right: 1px solid #e2e8f0;
                border-bottom: 2px solid #e2e8f0;
                font-weight: 700;
                color: #475569;
                font-size: 12px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            
            /* ========== PROGRESS BAR ========== */
            QProgressBar {
                background-color: #e2e8f0;
                border: none;
                border-radius: 6px;
                height: 12px;
                text-align: center;
                font-size: 10px;
                color: #475569;
            }
            QProgressBar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 #6366f1, stop:1 #8b5cf6);
                border-radius: 6px;
            }
            
            /* ========== SCROLLBARS ========== */
            QScrollBar:vertical {
                background-color: #f1f5f9;
                width: 12px;
                border-radius: 6px;
                margin: 2px;
            }
            QScrollBar::handle:vertical {
                background-color: #cbd5e1;
                border-radius: 5px;
                min-height: 30px;
            }
            QScrollBar::handle:vertical:hover {
                background-color: #94a3b8;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0px;
            }
            QScrollBar:horizontal {
                background-color: #f1f5f9;
                height: 12px;
                border-radius: 6px;
                margin: 2px;
            }
            QScrollBar::handle:horizontal {
                background-color: #cbd5e1;
                border-radius: 5px;
                min-width: 30px;
            }
            QScrollBar::handle:horizontal:hover {
                background-color: #94a3b8;
            }
            
            /* ========== SPLITTER ========== */
            QSplitter::handle {
                background-color: #e2e8f0;
            }
            QSplitter::handle:hover {
                background-color: #6366f1;
            }
            
            /* ========== LABELS ========== */
            QLabel {
                color: #374151;
                font-size: 13px;
            }
            
            /* ========== RADIO BUTTONS ========== */
            QRadioButton {
                color: #475569;
                font-size: 13px;
                spacing: 8px;
            }
            QRadioButton::indicator {
                width: 18px;
                height: 18px;
                border-radius: 9px;
                border: 2px solid #cbd5e1;
                background-color: #f8fafc;
            }
            QRadioButton::indicator:checked {
                background-color: #6366f1;
                border-color: #6366f1;
            }
            
            /* ========== FRAME ========== */
            QFrame {
                background-color: #ffffff;
                border-radius: 12px;
            }
            
            /* ========== TOOLTIPS ========== */
            QToolTip {
                background-color: #1e293b;
                color: #ffffff;
                border: none;
                border-radius: 6px;
                padding: 8px 12px;
                font-size: 12px;
            }
        """)
        
    def browse_output_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Chọn thư mục lưu kết quả")
        if folder:
            abs_path = os.path.abspath(folder)
            self.output_path.setText(abs_path)
            self.output_folder = abs_path
            for name in getattr(self, "media_subfolders", ["Images", "Videos"]):
                os.makedirs(os.path.join(self.output_folder, name), exist_ok=True)
            
    def open_output_folder(self):
        folder = self.output_path.text()
        if not os.path.exists(folder):
            os.makedirs(folder)
        if sys.platform == 'win32': os.startfile(folder)
        elif sys.platform == 'darwin': subprocess.run(['open', folder])
        else: subprocess.run(['xdg-open', folder])

    def on_thread_spin_changed(self, value):
        if getattr(self, "loading_config", False):
            return
        self.log_widget.add_log(f"Đã thay đổi Số luồng xử lý thành: {int(value)}", "info")

    def on_prompt_delay_changed(self, value):
        if getattr(self, "loading_config", False):
            return
        try:
            delay_value = float(value)
        except Exception:
            delay_value = float(self.prompt_delay_spin.value())
        self.prompt_delay = delay_value

    def on_direct_project_changed(self, state):
        if getattr(self, "loading_config", False):
            return
        enabled = self.direct_project_check.isChecked()
        text_state = "BẬT" if enabled else "TẮT"
        self.log_widget.add_log(f"Đã {text_state} chế độ 'Vào thẳng dự án'", "info")

    def on_hide_browser_changed(self, state):
        if getattr(self, "loading_config", False):
            return
        enabled = self.hide_browser_check.isChecked()
        text_state = "BẬT" if enabled else "TẮT"
        self.log_widget.add_log(f"Đã {text_state} chế độ 'Ẩn cửa sổ Chrome (chạy ngầm)'", "info")

    def on_upscale_changed(self, state):
        if getattr(self, "loading_config", False):
            return
        enabled = self.upscale_check.isChecked()
        text_state = "BẬT" if enabled else "TẮT"
        self.log_widget.add_log(f"Đã {text_state} chế độ 'Upscale 1080p'", "info")

    def on_video_mode_changed(self, mode_code):
        if getattr(self, "loading_config", False):
            return
        try:
            mode_text = self.video_tab.mode_combo.currentText()
        except Exception:
            mode_text = ""
        mode_str = str(mode_code) if mode_code is not None else ""
        if mode_str:
            message = f"Đã thay đổi Chế độ đầu vào Video thành: {mode_text} ({mode_str})"
        else:
            message = f"Đã thay đổi Chế độ đầu vào Video thành: {mode_text}"
        self.log_widget.add_log(message, "info")

    # --- LOGIC THÊM TASK VÀO HÀNG CHỜ (MỚI) ---
    def add_image_task_to_queue(self):
        prompts = self.image_tab.get_prompts()
        if not prompts:
            QMessageBox.warning(self, "Lỗi", "Vui lòng nhập Prompt!")
            return

        model = self.image_tab.model_combo.currentText()
        ratio = self.image_tab.ratio_combo.currentText()
        count = self.image_tab.count_spin.value()

        for prompt in prompts:
            task_id = f"IMG_{int(time.time())}_{len(self.result_table.tasks)+1}"
            self.result_table.add_task(task_id, model, ratio, prompt, count)
            self.result_table.set_task_mode(task_id, "Tạo hình ảnh")
            if task_id in self.result_table.tasks:
                self.result_table.tasks[task_id]["mode"] = "image"

        self.image_tab.prompt_text.clear()
        self.log_widget.add_log(f"Đã thêm {len(prompts)} task (x{count} ảnh) vào hàng chờ.", "info")

        if self.stop_btn.isEnabled():
            self.enqueue_new_waiting_tasks()

    def add_video_task_to_queue(self):
        tasks = self.video_tab.collect_tasks()
        if not tasks:
            return

        model = self.video_tab.model_combo.currentText()
        ratio = self.video_tab.ratio_combo.currentText()
        quality_is_1080 = self.video_tab.quality_combo.currentIndex() == 1

        added = 0
        for t in tasks:
            prompt = t.get("prompt", "").strip()
            if not prompt:
                continue
            count = int(t.get("count", 1)) if t.get("count", 1) else 1
            task_id = f"VID_{int(time.time())}_{len(self.result_table.tasks) + 1}"
            self.result_table.add_task(task_id, model, ratio, prompt, count)

            mode_code = t.get("mode")
            mode_label = self.video_tab.get_mode_display_label(mode_code) if mode_code else ""
            if mode_label:
                self.result_table.set_task_mode(task_id, mode_label)

            if task_id in self.result_table.tasks:
                info = self.result_table.tasks[task_id]
                info["mode"] = mode_code
                info["quality_1080"] = quality_is_1080

                extra = {}
                if mode_code == "image":
                    extra["image_path"] = t.get("image_path")
                elif mode_code == "start_end":
                    extra["start_image"] = t.get("start_image")
                    extra["end_image"] = t.get("end_image")
                elif mode_code == "reference":
                    extra["ref_images"] = t.get("ref_images") or []
                    extra["ref_keywords"] = t.get("ref_keywords") or []

                if extra:
                    info["video_extra"] = extra

            added += 1

        if added > 0:
            self.log_widget.add_log(f"Đã thêm {added} task video vào hàng chờ.", "info")
        else:
            QMessageBox.warning(self, "Lỗi", "Không có tác vụ video hợp lệ để thêm vào bảng.")
            return

        if self.stop_btn.isEnabled():
            self.enqueue_new_waiting_tasks()

    def enqueue_new_waiting_tasks(self):
        if not self.stop_btn.isEnabled():
            return
        if not self.worker_threads:
            return
        if not self.account_tab.accounts:
            return

        acc_count = len(self.account_tab.accounts)
        if acc_count <= 0:
            return

        existing_ids_in_queue = {t["id"] for t in getattr(self, "task_queue", [])}
        new_task_ids = [
            t_id for t_id, t_data in self.result_table.tasks.items()
            if t_data.get("status") == "waiting"
            and t_id not in self.enqueued_task_ids
            and t_id not in existing_ids_in_queue
        ]
        if not new_task_ids:
            return

        for task_id in new_task_ids:
            task_info = self.result_table.tasks[task_id]

            is_video = True if task_id.startswith("VID") else False
            video_quality_1080 = False
            if is_video:
                video_quality_1080 = bool(task_info.get("quality_1080", False))

            row = task_info["row"]
            stt_item = self.result_table.table.item(row, 1)
            stt_text = stt_item.text() if stt_item else ""
            try:
                order_index = int(stt_text)
            except Exception:
                order_index = row + 1

            total_tasks = len(self.result_table.tasks)
            task_data = {
                "id": task_id,
                "type": "unknown",
                "prompt": task_info["prompt"],
                "count": task_info.get("count", 1),
                "model": self.result_table.table.item(row, 4).text(),
                "ratio": self.result_table.table.item(row, 5).text(),
                "account_index": None,
                "output_folder": self.output_folder,
                "is_video": is_video,
                "mode": task_info.get("mode"),
                "video_quality_1080": video_quality_1080,
                "video_extra": task_info.get("video_extra", {}) if is_video else {},
                "order_index": order_index,
                "total_tasks": total_tasks
            }

            self.task_queue.append(task_data)

        self.dispatch_tasks(for_account_index=None)

        self.log_widget.add_log(
            f"Đã thêm {len(new_task_ids)} task mới vào hàng xử lý đang chạy.",
            "process"
        )

    def start_processing(self):
        text_path = self.output_path.text().strip()
        if text_path:
            self.output_folder = os.path.abspath(text_path)
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder, exist_ok=True)
        for name in getattr(self, "media_subfolders", ["Images", "Videos"]):
            os.makedirs(os.path.join(self.output_folder, name), exist_ok=True)

        existing_tasks = [
            t_id for t_id, t_data in self.result_table.tasks.items()
            if t_data.get("status") == "waiting"
        ]

        if not existing_tasks:
            self._is_start_processing = True
            try:
                current_tab = self.tab_widget.currentIndex()
                if current_tab == 0:
                    self.add_image_task_to_queue()
                elif current_tab == 1:
                    self.add_video_task_to_queue()

                existing_tasks = [
                    t_id for t_id, t_data in self.result_table.tasks.items()
                    if t_data.get("status") == "waiting"
                ]
            finally:
                self._is_start_processing = False

        if not existing_tasks:
            QMessageBox.warning(self, "Cảnh báo", "Không có tác vụ nào để xử lý!")
            return

        self.stop_all_workers()

        if not self.account_tab.accounts:
            QMessageBox.warning(self, "Cảnh báo", "Vui lòng thêm ít nhất 1 tài khoản!")
            return

        acc_count = len(self.account_tab.accounts)
        if acc_count <= 0:
            QMessageBox.warning(self, "Cảnh báo", "Không tìm thấy tài khoản hợp lệ!")
            return

        default_project_link = self.account_tab.project_link_input.text().strip()
        if default_project_link:
            for acc in self.account_tab.accounts:
                current_link = str(acc.get("project_link", "") or "").strip()
                if not current_link:
                    acc["project_link"] = default_project_link
                    row_index = acc.get("row_index")
                    if isinstance(row_index, int) and 0 <= row_index < self.account_tab.account_table.rowCount():
                        self.account_tab.account_table.setItem(row_index, 2, QTableWidgetItem(default_project_link))

        self.session_video_hashes = set()
        self.session_image_hashes = set()
        if hasattr(self, "prompt_delay_spin"):
            self.prompt_delay = float(self.prompt_delay_spin.value())
        else:
            self.prompt_delay = 0.5

        self.global_max_concurrent_tasks = max(1, int(self.thread_spin.value()))
        self.total_running_tasks = 0
        self.total_tasks = len(existing_tasks)
        self.completed_tasks = 0
        self.start_time = datetime.now()
        self.task_queue = []
        self.enqueued_task_ids = set()
        self.running_counts = {i: 0 for i in range(acc_count)}
        self.running_tasks_by_account = {i: [] for i in range(acc_count)}
        self.task_start_times = {}
        self.task_complete_times = {}
        self.account_ref_success = {}
        self.task_finished_by_manager = set()
        self.timeout_warned_tasks = set()
        self.blocked_accounts = set()

        for order_index, task_id in enumerate(existing_tasks, start=1):
            task_info = self.result_table.tasks[task_id]

            is_video = True if task_id.startswith("VID") else False
            video_quality_1080 = False
            if is_video:
                video_quality_1080 = bool(task_info.get("quality_1080", False))

            row = task_info["row"]
            task_data = {
                "id": task_id,
                "type": "unknown",
                "prompt": task_info["prompt"],
                "count": task_info.get("count", 1),
                "model": self.result_table.table.item(row, 4).text(),
                "ratio": self.result_table.table.item(row, 5).text(),
                "account_index": None,
                "output_folder": self.output_folder,
                "is_video": is_video,
                "mode": task_info.get("mode"),
                "video_quality_1080": video_quality_1080,
                "video_extra": task_info.get("video_extra", {}) if is_video else {},
                "order_index": order_index,
                "total_tasks": self.total_tasks
            }

            self.task_queue.append(task_data)

        acc_count = len(self.account_tab.accounts)
        num_threads = acc_count

        self.worker_threads = []
        self.account_workers = {}
        hide_browser = self.hide_browser_check.isChecked()
        for i in range(num_threads):
            worker = WorkerThread()
            worker.progress_update.connect(self.on_progress_update)
            worker.log_message.connect(self.log_widget.add_log)
            worker.task_complete.connect(self.on_task_complete)
            worker.task_error.connect(self.on_task_error)
            worker.captcha_detected.connect(self.on_worker_captcha_detected)
            worker.accounts = self.account_tab.accounts
            worker.hide_browser = hide_browser
            if hasattr(self, "direct_project_check"):
                worker.direct_project = self.direct_project_check.isChecked()
            worker.prompt_delay = getattr(self, "prompt_delay", 0.5)
            self.worker_threads.append(worker)
            self.account_workers[i] = worker
            worker.start()

        self.next_worker_index = 0

        self.dispatch_initial_tasks()

        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.log_widget.add_log(f"Bắt đầu xử lý {len(existing_tasks)} tác vụ...", "success")

    def dispatch_initial_tasks(self):
        self.dispatch_tasks(for_account_index=None)

    def dispatch_tasks(self, for_account_index=None):
        if not self.worker_threads:
            return
        if not hasattr(self, "task_queue"):
            self.task_queue = []
        if not self.account_tab.accounts:
            return
        acc_count = len(self.account_tab.accounts)
        if not hasattr(self, "running_counts") or not isinstance(self.running_counts, dict):
            self.running_counts = {i: 0 for i in range(acc_count)}
        if not hasattr(self, "running_tasks_by_account") or not isinstance(self.running_tasks_by_account, dict):
            self.running_tasks_by_account = {i: [] for i in range(acc_count)}
        if not hasattr(self, "next_worker_index"):
            self.next_worker_index = 0
        if not hasattr(self, "global_max_concurrent_tasks"):
            self.global_max_concurrent_tasks = max(1, int(self.thread_spin.value()))

        def can_run_on_account(task_data, acc_idx):
            blocked = getattr(self, "blocked_accounts", set())
            if acc_idx in blocked:
                return False
            current_list = self.running_tasks_by_account.get(acc_idx, [])
            is_1080_new = bool(task_data.get("video_quality_1080", False))
            has_1080_running = any(t.get("video_quality_1080") for t in current_list)
            if is_1080_new or has_1080_running:
                max_allowed = self.max_tasks_1080
            else:
                max_allowed = self.max_tasks_normal
            return len(current_list) < max_allowed

        while self.task_queue:
            if self.global_max_concurrent_tasks and getattr(self, "total_running_tasks", 0) >= self.global_max_concurrent_tasks:
                break
            blocked_accounts = getattr(self, "blocked_accounts", set())
            if for_account_index is not None:
                if for_account_index in blocked_accounts:
                    break
                candidate_accounts = [for_account_index]
            else:
                candidate_accounts = [i for i in range(acc_count) if i not in blocked_accounts]
            if not candidate_accounts:
                break
            assigned = False
            for acc_idx in candidate_accounts:
                if not self.task_queue:
                    break
                task_data = self.task_queue[0]
                if not can_run_on_account(task_data, acc_idx):
                    continue
                if not self.worker_threads:
                    return
                worker = None
                if hasattr(self, "account_workers") and acc_idx in self.account_workers:
                    worker = self.account_workers[acc_idx]
                else:
                    worker = self.worker_threads[self.next_worker_index % len(self.worker_threads)]
                    self.next_worker_index = (self.next_worker_index + 1) % len(self.worker_threads)
                if worker is None:
                    continue
                task_data["account_index"] = acc_idx
                task_id = task_data["id"]
                worker._current_task_ratio = task_data.get("ratio", "16:9")
                worker.add_task(task_data)
                start_ts = time.time()
                task_data["start_time"] = start_ts
                self.task_start_times[task_id] = start_ts
                self.running_counts[acc_idx] = self.running_counts.get(acc_idx, 0) + 1
                if acc_idx not in self.running_tasks_by_account:
                    self.running_tasks_by_account[acc_idx] = []
                self.running_tasks_by_account[acc_idx].append({
                    "id": task_id,
                    "video_quality_1080": task_data.get("video_quality_1080", False)
                })
                self.enqueued_task_ids.add(task_id)
                if task_id in self.result_table.tasks:
                    self.result_table.tasks[task_id]["account_index"] = acc_idx
                self.result_table.update_progress(task_id, 0, "processing", {})
                self.total_running_tasks = getattr(self, "total_running_tasks", 0) + 1
                self.task_queue.pop(0)
                assigned = True
            if not assigned:
                break
    
    def check_task_timeouts(self):
        try:
            if not self.stop_btn.isEnabled():
                return
            if not hasattr(self, "running_tasks_by_account"):
                return

            timeout_seconds = getattr(self, "task_timeout_seconds", 30)
            if not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
                return

            now = time.time()

            for acc_idx, running_list in self.running_tasks_by_account.items():
                if not running_list:
                    continue

                for info in list(running_list):
                    tid = info.get("id")
                    if not tid:
                        continue

                    if tid in self.timeout_warned_tasks:
                        continue

                    if tid in getattr(self, "task_finished_by_manager", set()):
                        continue

                    start_ts = self.task_start_times.get(tid, 0)
                    if not start_ts:
                        continue

                    elapsed = now - start_ts
                    if elapsed >= timeout_seconds:
                        self.timeout_warned_tasks.add(tid)
                        self.log_widget.add_log(
                            f"Task {tid} đã chạy {int(elapsed)} giây, vẫn đang chờ Flow xử lý (không đánh lỗi tự động).",
                            "warning"
                        )
        except Exception as e:
            try:
                self.log_widget.add_log(f"Lỗi nội bộ check_task_timeouts: {e}", "error")
            except Exception:
                pass

    def stop_all_workers(self, wait_for_finish=True):
        if not getattr(self, "worker_threads", None):
            self.worker_threads = []
            self.account_workers = {}
            self.enqueued_task_ids = set()
            return
        for worker in self.worker_threads:
            try:
                worker.stop()
            except Exception:
                pass
        if wait_for_finish:
            for worker in self.worker_threads:
                try:
                    worker.wait()
                except Exception:
                    pass
        self.worker_threads = []
        self.account_workers = {}
        if hasattr(self, "task_queue"):
            self.task_queue = []
        self.enqueued_task_ids = set()
        self.running_counts = {}
        self.running_tasks_by_account = {}
        self.total_running_tasks = 0
   
    def stop_processing(self):
        # Đánh dấu TẤT CẢ task (Đang xử lý / Đang chờ / Lỗi) thành Lỗi với lý do "Đã dừng bởi người dùng"
        if hasattr(self, "result_table") and hasattr(self.result_table, "tasks"):
            if not hasattr(self, "task_finished_by_manager"):
                self.task_finished_by_manager = set()

            for task_id, task_data in list(self.result_table.tasks.items()):
                current_status = task_data.get("status")
                if current_status in ("processing", "waiting", "error"):
                    self.task_finished_by_manager.add(task_id)
                    self.result_table.update_progress(
                        task_id,
                        0,
                        "error",
                        {
                            "error_type": "runtime_error",
                            "error_message": "Đã dừng bởi người dùng"
                        }
                    )

        # Dừng toàn bộ worker và xóa hàng đợi nội bộ
        self.stop_all_workers()

        # Cho phép người dùng bấm Start lại
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)

        # Ghi log 1 lần
        self.log_widget.add_log("Đã dừng toàn bộ tiến trình theo yêu cầu người dùng.", "warning")
        
    def handle_regenerate_task(self, task_id, new_prompt):
        if self.stop_btn.isEnabled():
            QMessageBox.warning(self, "Đang xử lý", "Vui lòng đợi các tác vụ hiện tại hoàn tất rồi hãy tạo lại.")
            return
        if not new_prompt or not new_prompt.strip():
            QMessageBox.warning(self, "Thiếu Prompt", "Prompt đang trống, vui lòng nhập prompt trước khi tạo lại.")
            return
        if task_id not in self.result_table.tasks:
            self.log_widget.add_log(f"Không tìm thấy task {task_id} để tạo lại.", "error")
            return
        self.result_table.update_task_prompt(task_id, new_prompt)
        self.result_table.mark_task_waiting(task_id)
        self.log_widget.add_log(f"Đã yêu cầu tạo lại task {task_id}.", "process")
        self.start_processing()

    def handle_run_single_task(self, task_id):
        text_path = self.output_path.text().strip()
        if text_path:
            self.output_folder = os.path.abspath(text_path)
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder, exist_ok=True)
        for name in getattr(self, "media_subfolders", ["Images", "Videos"]):
            os.makedirs(os.path.join(self.output_folder, name), exist_ok=True)

        if self.stop_btn.isEnabled():
            QMessageBox.warning(self, "Đang xử lý", "Vui lòng đợi các tác vụ hiện tại hoàn tất hoặc nhấn Dừng lại trước khi chạy từng hàng.")
            return
        if task_id not in self.result_table.tasks:
            self.log_widget.add_log(f"Không tìm thấy task {task_id} để chạy.", "error")
            return
        if not self.account_tab.accounts:
            QMessageBox.warning(self, "Cảnh báo", "Vui lòng thêm ít nhất 1 tài khoản!")
            return

        self.stop_all_workers()

        self.session_video_hashes = set()
        self.session_image_hashes = set()

        task_info = self.result_table.tasks[task_id]
        row = task_info["row"]
        account_index = 0

        is_video = True if task_id.startswith("VID") else False
        video_quality_1080 = False
        if is_video:
            video_quality_1080 = bool(task_info.get("quality_1080", False))
        task_data = {
            "id": task_id,
            "type": "single",
            "prompt": task_info["prompt"],
            "count": task_info.get("count", 1),
            "model": self.result_table.table.item(row, 4).text(),
            "ratio": self.result_table.table.item(row, 5).text(),
            "account_index": account_index,
            "output_folder": self.output_folder,
            "is_video": is_video,
            "mode": task_info.get("mode"),
            "video_quality_1080": video_quality_1080,
            "video_extra": task_info.get("video_extra", {}) if is_video else {},
            "order_index": 1,
            "total_tasks": 1
        }

        self.total_tasks = 1
        self.completed_tasks = 0
        self.start_time = datetime.now()
        self.task_queue = [task_data]
        self.running_counts = {0: 0}
        self.running_tasks_by_account = {0: []}
        self.task_start_times = {}
        self.task_complete_times = {}
        self.account_ref_success = {}
        self.task_finished_by_manager = set()
        self.timeout_warned_tasks = set()
        self.blocked_accounts = set()

        worker = WorkerThread()
        worker.progress_update.connect(self.on_progress_update)
        worker.log_message.connect(self.log_widget.add_log)
        worker.task_complete.connect(self.on_task_complete)
        worker.task_error.connect(self.on_task_error)
        worker.captcha_detected.connect(self.on_worker_captcha_detected)
        worker.accounts = self.account_tab.accounts
        worker.hide_browser = self.hide_browser_check.isChecked()
        if hasattr(self, "direct_project_check"):
            worker.direct_project = self.direct_project_check.isChecked()
        if hasattr(self, "prompt_delay_spin"):
            worker.prompt_delay = float(self.prompt_delay_spin.value())
        else:
            worker.prompt_delay = 0.5
        self.worker_threads = [worker]

        start_ts = time.time()
        task_data["start_time"] = start_ts
        self.task_start_times[task_id] = start_ts
        self.running_counts[account_index] = 1
        self.running_tasks_by_account[account_index] = [{
            "id": task_id,
            "video_quality_1080": video_quality_1080
        }]

        worker.start()
        worker.add_task(task_data)

        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.log_widget.add_log(f"Bắt đầu xử lý task {task_id}.", "success")

    def on_progress_update(self, task_id, progress, status, metadata):
        self.result_table.update_progress(task_id, progress, status, metadata)
        
    def on_worker_captcha_detected(self, account_index, message):
        if not isinstance(account_index, int):
            return
        if not hasattr(self, "blocked_accounts"):
            self.blocked_accounts = set()
        self.blocked_accounts.add(account_index)
        try:
            if hasattr(self.account_tab, "set_account_status"):
                self.account_tab.set_account_status(account_index, "Captcha - cần xác minh", "red", True)
        except Exception:
            pass
        try:
            self.log_widget.add_log(message, "warning")
        except Exception:
            pass

    def on_task_complete(self, task_id, prompt, results):
        if task_id in self.task_finished_by_manager:
            return

        self.task_complete_times[task_id] = time.time()
        self.completed_tasks += 1

        task_info = self.result_table.tasks.get(task_id)
        if isinstance(task_info, dict):
            acc_idx = task_info.get("account_index")
        else:
            acc_idx = None

        if isinstance(acc_idx, int):
            if acc_idx not in self.account_ref_success:
                self.account_ref_success[acc_idx] = {
                    "task_id": task_id,
                    "time": self.task_complete_times[task_id]
                }

        if isinstance(acc_idx, int) and hasattr(self, "running_counts") and hasattr(self, "running_tasks_by_account"):
            self.running_counts[acc_idx] = max(0, self.running_counts.get(acc_idx, 0) - 1)
            current_list = self.running_tasks_by_account.get(acc_idx, [])
            self.running_tasks_by_account[acc_idx] = [t for t in current_list if t.get("id") != task_id]
            if hasattr(self, "total_running_tasks"):
                self.total_running_tasks = max(0, self.total_running_tasks - 1)
            self.dispatch_tasks(for_account_index=acc_idx)

        is_video = task_id.startswith("VID")
        sub_folder = "Videos" if is_video else "Images"
        save_dir = os.path.join(self.output_folder, sub_folder)
        os.makedirs(save_dir, exist_ok=True)

        prompts_dir = getattr(self, "prompt_folder", os.path.join(self.output_folder, "Prompts"))
        os.makedirs(prompts_dir, exist_ok=True)
        try:
            with open(os.path.join(prompts_dir, "history.txt"), "a", encoding="utf-8") as f:
                f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {task_id} | {prompt}\n")
        except Exception:
            pass

        saved_files = []
        has_valid_data = False

        try:
            if isinstance(results, (list, tuple)):
                unique_results = []
                if is_video:
                    if not hasattr(self, "session_video_hashes"):
                        self.session_video_hashes = set()
                    seen_hashes = set()
                    for data in results:
                        if not isinstance(data, (bytes, bytearray)):
                            continue
                        b = bytes(data)
                        h = hashlib.sha1(b).hexdigest()
                        if h in seen_hashes or h in self.session_video_hashes:
                            continue
                        seen_hashes.add(h)
                        self.session_video_hashes.add(h)
                        unique_results.append(b)
                else:
                    if not hasattr(self, "session_image_hashes"):
                        self.session_image_hashes = set()
                    seen_hashes = set()
                    for data in results:
                        if not isinstance(data, (bytes, bytearray)):
                            continue
                        b = bytes(data)
                        h = hashlib.sha1(b).hexdigest()
                        if h in seen_hashes or h in self.session_image_hashes:
                            continue
                        seen_hashes.add(h)
                        self.session_image_hashes.add(h)
                        unique_results.append(b)
                    if not unique_results:
                        for data in results:
                            if isinstance(data, (bytes, bytearray)):
                                b = bytes(data)
                                h = hashlib.sha1(b).hexdigest()
                                self.session_image_hashes.add(h)
                                unique_results.append(b)
                                break

                results = unique_results
                total_files = len(results)
            else:
                total_files = 1

            if isinstance(results, (list, tuple)) and len(results) == 0:
                self.log_widget.add_log(
                    f"Không có dữ liệu hợp lệ để lưu cho task {task_id}.",
                    "warning"
                )
            else:
                has_valid_data = True

                self.log_widget.add_log(
                    f"Đang lưu {total_files} file cho task {task_id} vào thư mục: {sub_folder}",
                    "save"
                )

                self.cleanup_temp_media_files(task_id, sub_folder)

                existing_files = [f_name for f_name in os.listdir(save_dir) if f_name[0:3].isdigit() and "_" in f_name]
                max_idx = -1
                for f_name in existing_files:
                    try:
                        idx = int(f_name.split("_")[0])
                        if idx > max_idx:
                            max_idx = idx
                    except Exception:
                        continue

                next_idx = max_idx + 1
                prefix = f"{next_idx:03d}"

                def _save_one(data_bytes, index_suffix):
                    ext = ".mp4" if is_video else ".png"
                    file_name = f"{prefix}_{task_id}{index_suffix}{ext}"
                    output_path = os.path.join(save_dir, file_name)
                    with open(output_path, "wb") as f:
                        f.write(data_bytes)
                    saved_files.append(output_path)
                    self.log_widget.add_log(f"Đã lưu: {sub_folder}/{file_name}", "success")

                if isinstance(results, (list, tuple)):
                    for i, data in enumerate(results, start=1):
                        if not isinstance(data, (bytes, bytearray)):
                            continue
                        _save_one(data, f"_{i}")
                else:
                    if isinstance(results, (bytes, bytearray)):
                        _save_one(bytes(results), "")

        except Exception as e:
            self.log_widget.add_log(f"Lỗi lưu file {task_id}: {str(e)}", "error")

        if has_valid_data:
            self.result_table.set_preview(task_id, results, prompt, saved_files)

        if self.completed_tasks >= self.total_tasks:
            self.on_all_tasks_complete()

    def cleanup_temp_media_files(self, task_id, sub_folder):
        try:
            save_dir = os.path.join(self.output_folder, sub_folder)
            if not os.path.isdir(save_dir):
                return
            prefixes = [
                f"flow_img_task_{task_id}",
                f"flow_vid_task_{task_id}"
            ]
            for name in os.listdir(save_dir):
                for prefix in prefixes:
                    if name.startswith(prefix):
                        full_path = os.path.join(save_dir, name)
                        if os.path.isfile(full_path):
                            try:
                                os.remove(full_path)
                            except Exception:
                                pass
                        break
        except Exception:
            pass

    def cleanup_temp_files_on_exit(self):
        try:
            temp_dir = os.path.join(os.getcwd(), "temp_preview_videos")
            if os.path.isdir(temp_dir):
                for name in os.listdir(temp_dir):
                    full_path = os.path.join(temp_dir, name)
                    try:
                        if os.path.isfile(full_path):
                            os.remove(full_path)
                    except Exception:
                        pass
                try:
                    os.rmdir(temp_dir)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            for sub in getattr(self, "media_subfolders", ["Images", "Videos"]):
                folder = os.path.join(self.output_folder, sub)
                if not os.path.isdir(folder):
                    continue
                for name in os.listdir(folder):
                    if name.startswith("flow_img_task_") or name.startswith("flow_vid_task_"):
                        full_path = os.path.join(folder, name)
                        if os.path.isfile(full_path):
                            try:
                                os.remove(full_path)
                            except Exception:
                                pass
        except Exception:
            pass

    def handle_timeout_error(self, task_id, error_message):
        if task_id in self.task_finished_by_manager:
            return

        self.task_finished_by_manager.add(task_id)
        self.result_table.update_progress(
            task_id,
            0,
            "error",
            {"error_type": "prompt_timeout", "error_message": error_message}
        )
        self.log_widget.add_log(f"Task {task_id} lỗi (timeout): {error_message}", "error")

        task_info = self.result_table.tasks.get(task_id)
        if isinstance(task_info, dict):
            acc_idx = task_info.get("account_index")
        else:
            acc_idx = None

        if isinstance(acc_idx, int) and hasattr(self, "running_counts") and hasattr(self, "running_tasks_by_account"):
            self.running_counts[acc_idx] = max(0, self.running_counts.get(acc_idx, 0) - 1)
            current_list = self.running_tasks_by_account.get(acc_idx, [])
            self.running_tasks_by_account[acc_idx] = [t for t in current_list if t.get("id") != task_id]
            if hasattr(self, "total_running_tasks"):
                self.total_running_tasks = max(0, self.total_running_tasks - 1)
            self.dispatch_tasks(for_account_index=acc_idx)

        self.completed_tasks += 1
        if self.completed_tasks >= self.total_tasks:
            self.on_all_tasks_complete()

    def mark_all_tasks_error_for_account(self, account_index, reason_message: str):
        """
        Đánh dấu toàn bộ task thuộc account_index thành Lỗi
        và ghi log đúng 1 dòng theo mô tả.
        """
        try:
            # Log đúng 1 dòng theo yêu cầu
            self.log_widget.add_log(
                "Trình duyệt Chrome đã bị tắt – dừng toàn bộ task liên quan.",
                "error"
            )
        except Exception:
            pass

        # Duyệt tất cả task hiện có trong bảng
        for t_id, t_data in list(self.result_table.tasks.items()):
            acc_idx = t_data.get("account_index")
            status = t_data.get("status")
            if acc_idx == account_index and status in ("waiting", "processing"):
                # set finished bởi manager để worker hoàn thành sau không ghi đè
                self.task_finished_by_manager.add(t_id)
                # cập nhật trạng thái hiển thị = Lỗi
                self.result_table.update_progress(
                    t_id,
                    0,
                    "error",
                    {
                        "error_type": "runtime_error",
                        "error_message": reason_message or "Trình duyệt Chrome đã bị tắt."
                    }
                )

        # Cập nhật các cấu trúc đếm đang chạy để tránh dispatch thêm
        if hasattr(self, "running_tasks_by_account"):
            current_list = self.running_tasks_by_account.get(account_index, [])
            for info in current_list:
                t_id = info.get("id")
                if t_id:
                    self.task_finished_by_manager.add(t_id)
            self.running_tasks_by_account[account_index] = []

        if hasattr(self, "running_counts"):
            self.running_counts[account_index] = 0

        if hasattr(self, "total_running_tasks"):
            self.total_running_tasks = sum(self.running_counts.values()) if self.running_counts else 0

        # Không dispatch task mới cho account này nữa (tránh chạy trên Chrome đã tắt)
        if not hasattr(self, "blocked_accounts"):
            self.blocked_accounts = set()
        self.blocked_accounts.add(account_index)

    def on_task_error(self, task_id, error_message):
        if task_id in self.task_finished_by_manager:
            return

        # Kiểm tra task thuộc account nào
        task_info = self.result_table.tasks.get(task_id)
        if isinstance(task_info, dict):
            acc_idx = task_info.get("account_index")
        else:
            acc_idx = None

        # Nhận dạng trường hợp Chrome bị tắt (message đã thống nhất ở WorkerThread)
        chrome_closed_message = "Trình duyệt Chrome đã bị tắt – dừng toàn bộ task liên quan."
        msg_str = str(error_message) if error_message is not None else ""

        if msg_str.strip() == chrome_closed_message and isinstance(acc_idx, int):
            # Đánh dấu tất cả task của account này là lỗi + log 1 dòng chuẩn
            self.mark_all_tasks_error_for_account(acc_idx, chrome_closed_message)

            # Giảm số lượng đang chạy & xóa khỏi danh sách running_tasks_by_account
            if hasattr(self, "running_counts"):
                self.running_counts[acc_idx] = 0

            if hasattr(self, "running_tasks_by_account"):
                current_list = self.running_tasks_by_account.get(acc_idx, [])
                for info in current_list:
                    t_id = info.get("id")
                    if t_id:
                        self.task_finished_by_manager.add(t_id)
                self.running_tasks_by_account[acc_idx] = []

            if hasattr(self, "total_running_tasks"):
                self.total_running_tasks = sum(self.running_counts.values()) if self.running_counts else 0

            # Không dispatch task mới cho account này nữa
            if not hasattr(self, "blocked_accounts"):
                self.blocked_accounts = set()
            self.blocked_accounts.add(acc_idx)

            # Không tăng completed_tasks ở đây vì mỗi task đã được đánh lỗi riêng
            return

        # --- HÀNH VI CŨ CHO CÁC LỖI KHÁC GIỮ NGUYÊN --- 
        if isinstance(error_message, str) and error_message.startswith("FLOW_ERROR:"):
            clean_msg = error_message[len("FLOW_ERROR:"):].strip()
            error_type = "prompt_error"
            display_message = clean_msg if clean_msg else error_message
        else:
            error_type = "runtime_error"
            display_message = error_message

        self.completed_tasks += 1
        self.result_table.update_progress(
            task_id,
            0,
            "error",
            {"error_type": error_type, "error_message": display_message}
        )
        self.log_widget.add_log(f"Task {task_id} lỗi: {display_message}", "error")

        if isinstance(acc_idx, int):
            msg_low = display_message.lower() if isinstance(display_message, str) else ""
            if (
                "cookie hết hạn" in msg_low
                or "chưa đăng nhập" in msg_low
                or "login lại" in msg_low
            ):
                if hasattr(self.account_tab, "update_status_login_required"):
                    self.account_tab.update_status_login_required(acc_idx)

        if isinstance(acc_idx, int) and hasattr(self, "running_counts") and hasattr(self, "running_tasks_by_account"):
            self.running_counts[acc_idx] = max(0, self.running_counts.get(acc_idx, 0) - 1)
            current_list = self.running_tasks_by_account.get(acc_idx, [])
            self.running_tasks_by_account[acc_idx] = [t for t in current_list if t.get("id") != task_id]
            if hasattr(self, "total_running_tasks"):
                self.total_running_tasks = max(0, self.total_running_tasks - 1)
            self.dispatch_tasks(for_account_index=acc_idx)

        if self.completed_tasks >= self.total_tasks:
            self.on_all_tasks_complete()
            
    def on_all_tasks_complete(self):
        self.stop_all_workers(wait_for_finish=True)
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        elapsed_time = datetime.now() - self.start_time
        self.log_widget.add_log(
            f"Hoàn thành tất cả {self.total_tasks} tác vụ trong {elapsed_time}",
            "success"
        )
        QMessageBox.information(
            self,
            "Hoàn thành",
            f"Đã xử lý xong {self.total_tasks} tác vụ!"
        )
            
    def save_config(self, silent=False):
        if hasattr(self, 'main_splitter'):
            if self._saved_main_sizes:
                main_sizes = self._saved_main_sizes
            else:
                main_sizes = self.main_splitter.sizes()
        else:
            main_sizes = None

        config = {
            'image_model': self.image_tab.model_combo.currentText(),
            'image_ratio': self.image_tab.ratio_combo.currentText(),
            'image_count': self.image_tab.count_spin.value(),
            'video_model': self.video_tab.model_combo.currentText(),
            'threads': self.thread_spin.value(),
            'prompt_delay': float(self.prompt_delay_spin.value()),
            'task_timeout_seconds': self.task_timeout_seconds,
            'output_folder': self.output_path.text(),
            'upscale': self.upscale_check.isChecked(),
            'direct_project': self.direct_project_check.isChecked(),
            'chrome_profile': self.account_tab.profile_path.text(),
            'headless': self.account_tab.headless_check.isChecked(),
            'hide_browser': self.hide_browser_check.isChecked(),
            'table_columns': self.result_table.get_column_widths(),
            'account_table_columns': self.account_tab.get_column_widths(),
            'main_splitter': main_sizes,
            'right_splitter': self.right_splitter.sizes() if hasattr(self, 'right_splitter') else None,
            'accounts': self.account_tab.export_accounts(),
            'gemini_api_key': GeminiAPIManager.get_api_key(),
            'gemini_model': GeminiAPIManager.get_model()
        }

        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
            self.log_widget.add_log("Đã lưu cấu hình", "success")
            if not silent:
                QMessageBox.information(self, "Thành công", "Đã lưu cấu hình thành công!")
        except Exception as e:
            self.log_widget.add_log(f"Lỗi lưu cấu hình: {str(e)}", "error")
            if not silent:
                QMessageBox.critical(self, "Lỗi", f"Không thể lưu cấu hình: {str(e)}")
            
    def load_config(self):
        if not os.path.exists(self.config_file): return
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)

            if 'image_model' in config:
                idx = self.image_tab.model_combo.findText(config['image_model'])
                if idx >= 0:
                    self.image_tab.model_combo.setCurrentIndex(idx)

            if 'image_ratio' in config:
                idx = self.image_tab.ratio_combo.findText(config['image_ratio'])
                if idx >= 0:
                    self.image_tab.ratio_combo.setCurrentIndex(idx)

            if 'image_count' in config:
                self.image_tab.count_spin.setValue(int(config['image_count']))

            if 'video_model' in config:
                idx = self.video_tab.model_combo.findText(config['video_model'])
                if idx >= 0:
                    self.video_tab.model_combo.setCurrentIndex(idx)

            if 'threads' in config:
                self.thread_spin.setValue(int(config['threads']))

            if 'prompt_delay' in config and hasattr(self, 'prompt_delay_spin'):
                try:
                    self.prompt_delay_spin.setValue(float(config['prompt_delay']))
                except Exception:
                    pass
            if 'task_timeout_seconds' in config:
                try:
                    self.task_timeout_seconds = int(config['task_timeout_seconds'])
                except Exception:
                    pass
            if 'output_folder' in config: 
                self.output_path.setText(config['output_folder'])
                self.output_folder = config['output_folder']
            if 'upscale' in config:
                self.upscale_check.setChecked(config['upscale'])
            if 'direct_project' in config and hasattr(self, 'direct_project_check'):
                self.direct_project_check.setChecked(config['direct_project'])
            if 'chrome_profile' in config:
                self.account_tab.profile_path.setText(config['chrome_profile'])
            if 'headless' in config:
                self.account_tab.headless_check.setChecked(config['headless'])
            if 'hide_browser' in config:
                self.hide_browser_check.setChecked(config['hide_browser'])
            if 'default_project_link' in config:
                self.account_tab.project_link_input.setText(config['default_project_link'])
            if 'table_columns' in config: self.result_table.set_column_widths(config['table_columns'])
            if 'account_table_columns' in config:
                self.account_tab.set_column_widths(config['account_table_columns'])
            if 'main_splitter' in config and config['main_splitter']:
                try:
                    self.main_splitter.setSizes(config['main_splitter'])
                    self._saved_main_sizes = config['main_splitter']
                except Exception:
                    pass
            if 'right_splitter' in config and config['right_splitter']:
                try:
                    self.right_splitter.setSizes(config['right_splitter'])
                except Exception:
                    pass
            if 'accounts' in config:
                self.account_tab.load_from_data(config['accounts'])
                self.log_widget.add_log(
                    "Đã tải danh sách tài khoản từ cấu hình. Khuyến khích dán COOKIE MỚI trước khi chạy để tránh die.",
                    "warning"
                )
            # Load Gemini API settings
            if 'gemini_api_key' in config and config['gemini_api_key']:
                GeminiAPIManager.set_api_key(config['gemini_api_key'])
                if hasattr(self, 'gemini_settings_tab'):
                    self.gemini_settings_tab.api_key_input.setText(config['gemini_api_key'])
            if 'gemini_model' in config and config['gemini_model']:
                GeminiAPIManager.set_model(config['gemini_model'])
                if hasattr(self, 'gemini_settings_tab'):
                    idx = self.gemini_settings_tab.model_combo.findText(config['gemini_model'])
                    if idx >= 0:
                        self.gemini_settings_tab.model_combo.setCurrentIndex(idx)
            self.log_widget.add_log("Đã tải cấu hình từ file", "info")
        except Exception as e:
            self.log_widget.add_log(f"Lỗi tải cấu hình: {str(e)}", "warning")
            
    def closeEvent(self, event):
        self.log_widget.add_log("Đang dọn dẹp tiến trình...", "warning")
        try:
            self.save_config(silent=True)
        except Exception:
            pass
        try:
            self.timeout_timer.stop()
        except Exception:
            pass
        self.stop_all_workers(wait_for_finish=True)
        if sys.platform == 'win32':
            try:
                subprocess.run(
                    ['taskkill', '/F', '/IM', 'chromedriver.exe'],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            except Exception:
                pass
        try:
            self.cleanup_temp_files_on_exit()
        except Exception:
            pass
        event.accept()

# --- CODE ĐÃ SỬA LOGIC LOGIN ---
class AccountTab(QWidget):
    def __init__(self):
        super().__init__()
        self.accounts = []
        self.init_ui()
        
    def init_ui(self):
        # Sử dụng layout chính là VBox
        layout = QVBoxLayout()
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(16)
        
        # Header
        header_layout = QHBoxLayout()
        header_icon = QLabel("👤")
        header_icon.setStyleSheet("font-size: 24px;")
        header_title = QLabel("Quản Lý Tài Khoản")
        header_title.setStyleSheet("font-size: 18px; font-weight: bold; color: #1e293b;")
        header_layout.addWidget(header_icon)
        header_layout.addWidget(header_title)
        header_layout.addStretch()
        layout.addLayout(header_layout)
        
        # --- Phần 1: Login Card ---
        login_group = QFrame()
        login_group.setObjectName("loginCard")
        login_group.setStyleSheet("""
            #loginCard {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #fafafa, stop:1 #f5f5f5);
                border: 1px solid #e5e7eb;
                border-radius: 12px;
                padding: 12px;
            }
        """)
        login_layout = QVBoxLayout(login_group)
        login_layout.setContentsMargins(16, 12, 16, 12)
        login_layout.setSpacing(12)
        
        login_header = QHBoxLayout()
        login_icon = QLabel("🔐")
        login_icon.setStyleSheet("font-size: 16px;")
        login_label = QLabel("Phương thức đăng nhập")
        login_label.setStyleSheet("font-size: 14px; font-weight: 600; color: #475569;")
        login_header.addWidget(login_icon)
        login_header.addWidget(login_label)
        login_header.addStretch()
        login_layout.addLayout(login_header)
        
        self.login_method_group = QButtonGroup()
        self.google_login_radio = QRadioButton("🌐 Login Google (Mở trình duyệt)")
        self.cookie_login_radio = QRadioButton("🍪 Login Cookie (Thủ công)")
        self.cookie_login_radio.setChecked(True)
        
        self.google_login_radio.toggled.connect(self.toggle_login_ui)
        
        self.login_method_group.addButton(self.google_login_radio)
        self.login_method_group.addButton(self.cookie_login_radio)
        
        radio_layout = QHBoxLayout()
        radio_layout.setSpacing(20)
        radio_layout.addWidget(self.google_login_radio)
        radio_layout.addWidget(self.cookie_login_radio)
        radio_layout.addStretch()
        login_layout.addLayout(radio_layout)
        
        self.cookie_widget = QWidget()
        cookie_layout = QVBoxLayout(self.cookie_widget)
        cookie_layout.setContentsMargins(0, 8, 0, 0)
        cookie_layout.setSpacing(10)

        self.cookie_text = QTextEdit()
        self.cookie_text.setPlaceholderText("Dán Cookie vào đây...")
        self.cookie_text.setMaximumHeight(70)
        cookie_layout.addWidget(self.cookie_text)

        project_row = QHBoxLayout()
        project_label = QLabel("🔗 Link Project:")
        project_label.setStyleSheet("font-weight: 600; color: #475569;")
        project_row.addWidget(project_label)
        self.project_link_input = QLineEdit()
        self.project_link_input.setPlaceholderText("https://labs.google/fx/vi/tools/flow/project/...")
        project_row.addWidget(self.project_link_input)
        cookie_layout.addLayout(project_row)

        btn_row = QHBoxLayout()
        btn_row.setSpacing(10)
        
        self.import_cookie_btn = QPushButton("📂 Import File")
        self.import_cookie_btn.setCursor(Qt.PointingHandCursor)
        self.import_cookie_btn.setStyleSheet("""
            QPushButton { 
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #10B981, stop:1 #059669);
                color: white; 
                padding: 10px 16px; 
                border-radius: 8px;
                font-weight: 600;
                border: none;
            }
            QPushButton:hover { 
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #059669, stop:1 #047857);
            }
        """)
        self.import_cookie_btn.clicked.connect(self.import_cookies)
        btn_row.addWidget(self.import_cookie_btn)

        self.add_account_btn = QPushButton("💾 Lưu / Thêm TK")
        self.add_account_btn.setCursor(Qt.PointingHandCursor)
        self.add_account_btn.setStyleSheet("""
            QPushButton { 
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #3b82f6, stop:1 #2563eb);
                color: white; 
                padding: 10px 16px; 
                border-radius: 8px;
                font-weight: 600;
                border: none;
            }
            QPushButton:hover { 
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #2563eb, stop:1 #1d4ed8);
            }
        """)
        self.add_account_btn.clicked.connect(self.add_account)
        btn_row.addWidget(self.add_account_btn)
        btn_row.addStretch()

        cookie_layout.addLayout(btn_row)
        login_layout.addWidget(self.cookie_widget)
        
        self.google_login_btn = QPushButton("🌍 Mở Form Đăng Nhập Google")
        self.google_login_btn.setCursor(Qt.PointingHandCursor)
        self.google_login_btn.setStyleSheet("""
            QPushButton { 
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #ef4444, stop:1 #dc2626);
                color: white; 
                padding: 12px 20px; 
                border-radius: 8px;
                font-weight: 700;
                border: none;
            }
            QPushButton:hover { 
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #dc2626, stop:1 #b91c1c);
            }
        """)
        self.google_login_btn.clicked.connect(self.open_google_login)
        self.google_login_btn.setVisible(False)
        login_layout.addWidget(self.google_login_btn)
        
        layout.addWidget(login_group)

        # Khởi tạo biến ẩn để tránh lỗi
        self.profile_path = QLineEdit()
        self.headless_check = QCheckBox()
        
        # --- Phần 2: Bảng Tài Khoản ---
        table_header = QHBoxLayout()
        table_icon = QLabel("📊")
        table_icon.setStyleSheet("font-size: 16px;")
        account_label = QLabel("Danh sách Tài Khoản")
        account_label.setStyleSheet("font-size: 14px; font-weight: 600; color: #1e293b;")
        table_header.addWidget(table_icon)
        table_header.addWidget(account_label)
        table_header.addStretch()
        layout.addLayout(table_header)
        
        self.account_table = QTableWidget()
        self.account_table.setColumnCount(5)
        self.account_table.setHorizontalHeaderLabels(["Email/ID", "Profile Chrome", "Link Project", "Trạng thái", "Hành động"])
        header = self.account_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        self.account_table.setColumnWidth(4, 280)
        self.account_table.itemChanged.connect(self.on_table_item_changed)
        layout.addWidget(self.account_table, stretch=1)
        
        # Nút Check nằm dưới cùng
        check_btn = QPushButton("🔍 Check Live All & Credit")
        check_btn.setCursor(Qt.PointingHandCursor)
        check_btn.setStyleSheet("""
            QPushButton { 
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #f59e0b, stop:1 #d97706);
                color: white; 
                padding: 14px 28px; 
                border-radius: 10px;
                font-size: 14px;
                font-weight: 700;
                border: none;
            }
            QPushButton:hover { 
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #d97706, stop:1 #b45309);
            }
        """)
        check_btn.clicked.connect(self.check_all_accounts)
        layout.addWidget(check_btn)
        
        self.setLayout(layout)
    
    def export_accounts(self):
        data = []
        for acc in self.accounts:
            data.append({
                "type": acc.get("type", "cookie"),
                "cookie": acc.get("cookie", ""),
                "profile_dir": acc.get("profile_dir", ""),
                "email": acc.get("email", ""),
                "project_link": acc.get("project_link", "")
            })
        return data

    def load_from_data(self, data_list):
        self.accounts = []
        self.account_table.setRowCount(0)
        if not data_list:
            return

        for acc in data_list:
            email_display = acc.get("email", f"Account {len(self.accounts) + 1}")
            profile_dir = acc.get("profile_dir", "")
            cookie_val = acc.get("cookie", "")
            account_type = acc.get("type", "cookie")
            project_link = acc.get("project_link", "")

            row = self.account_table.rowCount()
            self.account_table.insertRow(row)
            self.account_table.setRowHeight(row, 32)

            self.account_table.setItem(row, 0, QTableWidgetItem(email_display))
            self.account_table.setItem(row, 1, QTableWidgetItem(profile_dir))
            self.account_table.setItem(row, 2, QTableWidgetItem(project_link))
            self.account_table.setItem(row, 3, QTableWidgetItem("Chưa check"))

            action_widget = QWidget()
            action_layout = QHBoxLayout(action_widget)
            action_layout.setContentsMargins(0, 0, 0, 0)
            action_layout.setSpacing(4)
            action_layout.setAlignment(Qt.AlignCenter)
            action_widget.setMinimumHeight(30)

            open_folder_btn = QPushButton("📂 Folder")
            open_folder_btn.setToolTip("Mở thư mục profile")
            open_folder_btn.setMinimumWidth(80)
            open_folder_btn.setMinimumHeight(26)
            open_folder_btn.clicked.connect(lambda _, r=row: self.open_profile_folder(r))
            action_layout.addWidget(open_folder_btn)
            action_layout.setAlignment(open_folder_btn, Qt.AlignCenter)

            open_chrome_btn = QPushButton("🌐 Chrome")
            open_chrome_btn.setToolTip("Mở Chrome với profile này")
            open_chrome_btn.setMinimumWidth(90)
            open_chrome_btn.setMinimumHeight(26)
            open_chrome_btn.clicked.connect(lambda _, r=row: self.open_profile_in_chrome(r))
            action_layout.addWidget(open_chrome_btn)
            action_layout.setAlignment(open_chrome_btn, Qt.AlignCenter)

            save_link_btn = QPushButton("💾 Lưu link")
            save_link_btn.setToolTip("Lưu link project cho dòng này")
            save_link_btn.setMinimumWidth(90)
            open_chrome_btn.setMinimumHeight(26)
            save_link_btn.clicked.connect(lambda _, r=row: self.save_project_link_row(r))
            action_layout.addWidget(save_link_btn)
            action_layout.setAlignment(open_chrome_btn, Qt.AlignCenter)

            delete_btn = QPushButton("✖ Xóa")
            delete_btn.setToolTip("Xóa tài khoản này")
            delete_btn.setMinimumWidth(60)
            open_chrome_btn.setMinimumHeight(26)
            delete_btn.setStyleSheet("color: red; font-weight: bold;")
            delete_btn.clicked.connect(lambda _, r=row: self.delete_account_row(r))
            action_layout.addWidget(delete_btn)
            action_layout.setAlignment(open_chrome_btn, Qt.AlignCenter)

            self.account_table.setCellWidget(row, 4, action_widget)

            self.accounts.append({
                "type": account_type,
                "cookie": cookie_val,
                "profile_dir": profile_dir,
                "email": email_display,
                "project_link": project_link,
                "row_index": row
            })

    def toggle_login_ui(self):
        # Logic ẩn hiện giao diện khi chọn Radio Button
        if self.google_login_radio.isChecked():
            # Khi chọn Login Google: Hiện nút mở web + Hiện chỗ dán cookie
            self.google_login_btn.setVisible(True)
            self.cookie_widget.setVisible(True) 
            self.cookie_text.setPlaceholderText("Bước 1: Nhấn nút đỏ trên để đăng nhập.\nBước 2: Copy Cookie và dán vào đây...")
        else:
            # Khi chọn Login Cookie: Ẩn nút mở web + Hiện chỗ dán cookie
            self.google_login_btn.setVisible(False)
            self.cookie_widget.setVisible(True)
            self.cookie_text.setPlaceholderText("Nhập cookie thủ công tại đây...")

    def open_google_login(self):
        # Mở trình duyệt hệ thống để đăng nhập
        import webbrowser
        url = "https://labs.google/" # Link Google Flow
        try:
            webbrowser.open(url)
            QMessageBox.information(self, "Hướng dẫn", 
                "Đã mở trình duyệt!\n\n1. Hãy đăng nhập tài khoản Google của bạn.\n2. Sau khi đăng nhập xong, hãy dùng Extension 'Cookie-Editor' để copy cookie.\n3. Chọn lại 'Login Cookie' trên tool và dán vào.")
        except Exception as e:
            QMessageBox.critical(self, "Lỗi", f"Không thể mở trình duyệt: {str(e)}")

    def browse_profile(self):
        folder = QFileDialog.getExistingDirectory(self, "Chọn thư mục Profile Chrome")
        if folder:
            self.profile_path.setText(folder)

    def open_profile_folder(self, row):
        item = self.account_table.item(row, 1)
        if not item:
            QMessageBox.warning(self, "Lỗi", "Không tìm thấy đường dẫn profile cho dòng này.")
            return

        folder = item.text().strip()
        if not folder:
            QMessageBox.warning(self, "Lỗi", "Dòng này chưa có đường dẫn profile.")
            return

        if not os.path.exists(folder):
            QMessageBox.warning(self, "Lỗi", f"Thư mục profile không tồn tại:\n{folder}")
            return

        if sys.platform == "win32":
            os.startfile(folder)
        elif sys.platform == "darwin":
            subprocess.run(["open", folder])
        else:
            subprocess.run(["xdg-open", folder])

    def open_profile_in_chrome(self, row):
        item = self.account_table.item(row, 1)
        if not item:
            QMessageBox.warning(self, "Lỗi", "Không tìm thấy đường dẫn profile cho dòng này.")
            return

        folder = item.text().strip()
        if not folder:
            QMessageBox.warning(self, "Lỗi", "Dòng này chưa có đường dẫn profile.")
            return

        if not os.path.exists(folder):
            QMessageBox.warning(self, "Lỗi", f"Thư mục profile không tồn tại:\n{folder}")
            return

        chrome_exe = None
        if sys.platform == "win32":
            possible_paths = [
                r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
            ]
            for p in possible_paths:
                if os.path.exists(p):
                    chrome_exe = p
                    break
        else:
            chrome_exe = "google-chrome"

        if not chrome_exe:
            QMessageBox.warning(
                self,
                "Lỗi",
                "Không tìm thấy file chạy Chrome.\nBạn hãy mở Chrome và chọn profile này thủ công."
            )
            self.open_profile_folder(row)
            return

        flow_url = "https://labs.google/fx/vi/tools/flow"

        try:
            subprocess.Popen([chrome_exe, f"--user-data-dir={folder}", "--profile-directory=Default", flow_url])
        except Exception as e:
            QMessageBox.critical(self, "Lỗi", f"Không mở được Chrome:\n{e}")
            
    def import_cookies(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Chọn file Cookie", "", "Text Files (*.txt);;CSV Files (*.csv)")
        if file_path:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    cookies = f.read()
                    self.cookie_text.setText(cookies)
                QMessageBox.information(self, "Thành công", f"Đã import cookie từ {os.path.basename(file_path)}")
            except Exception as e:
                QMessageBox.critical(self, "Lỗi", f"Không thể đọc file: {str(e)}")
                
    def add_account(self):
        project_link = self.project_link_input.text().strip()

        if self.google_login_radio.isChecked():
            profile_dir = self.profile_path.text().strip()
            if not profile_dir:
                QMessageBox.warning(self, "Thiếu Profile", "Vui lòng chọn thư mục Profile Chrome.")
                return
            email_display = os.path.basename(profile_dir) or f"Profile {len(self.accounts) + 1}"
            cookie_val = ""
            account_type = "profile"
        else:
            raw_input = self.cookie_text.toPlainText().strip()
            if not raw_input:
                QMessageBox.warning(self, "Cảnh báo", "Vui lòng nhập cookie!")
                return

            clean_cookie_str = unquote(raw_input).replace('"', "").replace("%22", "")
            email_match = re.search(r'([a-zA-Z0-9._+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', clean_cookie_str)
            if email_match:
                email_display = email_match.group(1)
            else:
                email_display = f"Account {len(self.accounts) + 1}"

            if raw_input.strip().startswith("{") or raw_input.strip().startswith("["):
                cookie_val = raw_input.strip()
            else:
                cookie_val = unquote(raw_input)

            profiles_root = os.path.join(os.path.abspath(os.getcwd()), "browser_profiles")
            safe_folder = "".join([c for c in email_display if c.isalnum()]) or f"user_{len(self.accounts) + 1}"
            profile_dir = os.path.join(profiles_root, safe_folder)
            account_type = "cookie"

        if profile_dir and not os.path.exists(profile_dir):
            os.makedirs(profile_dir, exist_ok=True)

        row = self.account_table.rowCount()
        self.account_table.insertRow(row)
        self.account_table.setRowHeight(row, 32)

        self.account_table.setItem(row, 0, QTableWidgetItem(email_display))
        self.account_table.setItem(row, 1, QTableWidgetItem(profile_dir))
        self.account_table.setItem(row, 2, QTableWidgetItem(project_link))
        self.account_table.setItem(row, 3, QTableWidgetItem("Chờ check..."))

        action_widget = QWidget()
        action_layout = QHBoxLayout(action_widget)
        action_layout.setContentsMargins(2, 2, 2, 2)
        action_layout.setSpacing(4)
        action_layout.setAlignment(Qt.AlignCenter)

        open_folder_btn = QPushButton("📂 Folder")
        open_folder_btn.setToolTip("Mở thư mục profile")
        open_folder_btn.setMinimumWidth(80)
        open_folder_btn.clicked.connect(lambda _, r=row: self.open_profile_folder(r))
        action_layout.addWidget(open_folder_btn)

        open_chrome_btn = QPushButton("🌐 Chrome")
        open_chrome_btn.setToolTip("Mở Chrome với profile này")
        open_chrome_btn.setMinimumWidth(90)
        open_chrome_btn.clicked.connect(lambda _, r=row: self.open_profile_in_chrome(r))
        action_layout.addWidget(open_chrome_btn)

        save_link_btn = QPushButton("💾 Lưu link")
        save_link_btn.setToolTip("Lưu link project cho dòng này")
        save_link_btn.setMinimumWidth(90)
        save_link_btn.clicked.connect(lambda _, r=row: self.save_project_link_row(r))
        action_layout.addWidget(save_link_btn)

        delete_btn = QPushButton("✖ Xóa")
        delete_btn.setToolTip("Xóa tài khoản này")
        delete_btn.setMinimumWidth(60)
        delete_btn.setStyleSheet("color: red; font-weight: bold;")
        delete_btn.clicked.connect(lambda _, r=row: self.delete_account_row(r))
        action_layout.addWidget(delete_btn)

        self.account_table.setCellWidget(row, 4, action_widget)

        self.accounts.append({
            "type": account_type,
            "cookie": cookie_val,
            "profile_dir": profile_dir,
            "email": email_display,
            "project_link": project_link,
            "row_index": row
        })
        self.cookie_text.clear()
        
    def update_account_table(self):
        pass
    
    def on_table_item_changed(self, item):
        row = item.row()
        col = item.column()
        if row < 0 or row >= len(self.accounts):
            return
        text = item.text().strip()
        acc = self.accounts[row]
        if col == 0:
            acc["email"] = text
        elif col == 1:
            acc["profile_dir"] = text
        elif col == 2:
            acc["project_link"] = text

    def save_project_link_row(self, row):
        if row < 0 or row >= self.account_table.rowCount():
            return
        item = self.account_table.item(row, 2)
        if not item:
            return
        link = item.text().strip()
        if row < len(self.accounts):
            self.accounts[row]["project_link"] = link
        parent = self.window()
        if hasattr(parent, "save_config"):
            try:
                parent.save_config(silent=True)
            except TypeError:
                parent.save_config()

    def delete_account_row(self, row):
        if row < 0 or row >= self.account_table.rowCount():
            return
        self.account_table.removeRow(row)
        if row < len(self.accounts):
            self.accounts.pop(row)

    def remove_account(self, index):
        self.delete_account_row(index)

    def set_account_status(self, index, text, color_name=None, bold=False):
        if index < 0 or index >= self.account_table.rowCount():
            return
        item = QTableWidgetItem(text)
        if color_name:
            item.setForeground(QColor(color_name))
        if bold:
            item.setFont(QFont("Arial", 9, QFont.Bold))
        self.account_table.setItem(index, 3, item)

    def update_status_login_required(self, index):
        self.set_account_status(index, "Yêu cầu đăng nhập lại", "red", True)

    def get_column_widths(self):
        widths = []
        for i in range(self.account_table.columnCount()):
            widths.append(self.account_table.columnWidth(i))
        return widths

    def set_column_widths(self, widths):
        if not widths:
            return
        for i, w in enumerate(widths):
            if i < self.account_table.columnCount():
                self.account_table.setColumnWidth(i, w)
    
    def _check_cookie_live(self, target_url, raw_cookie):
        try:
            session = requests.Session()
            formatted_cookies = {}
            raw_cookie_clean = raw_cookie.strip()
            if raw_cookie_clean.startswith("[") or raw_cookie_clean.startswith("{"):
                try:
                    json_data = json.loads(raw_cookie_clean)
                    if isinstance(json_data, list):
                        cookie_list = json_data
                    else:
                        cookie_list = json_data.get("cookies", [json_data])
                    for c in cookie_list:
                        if isinstance(c, dict) and c.get("name") and c.get("value"):
                            formatted_cookies[c.get("name")] = str(c.get("value")).replace('"', "")
                except Exception:
                    formatted_cookies = {}
            else:
                for item in raw_cookie_clean.split(";"):
                    if "=" in item:
                        name, value = item.split("=", 1)
                        formatted_cookies[name.strip()] = value.strip().replace('"', "")
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://labs.google/"
            }
            resp = session.get(target_url, cookies=formatted_cookies, headers=headers, timeout=20, allow_redirects=True)
            if "accounts.google.com" in resp.url or "ServiceLogin" in resp.url:
                return False, None, "Yêu cầu đăng nhập lại (cookie redirect)"
            if resp.status_code != 200:
                return False, None, f"Lỗi HTTP {resp.status_code}"
            page_content = resp.text
            found_emails = re.findall(r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", page_content)
            ignore_list = [
                "labs-google-fx",
                "noreply",
                "support",
                "google.com",
                "w3.org",
                "recaptcha",
                "gstatic",
                "googleusercontent"
            ]
            valid_email = None
            for e in found_emails:
                if not any(x in e for x in ignore_list):
                    valid_email = e
                    break
            return True, valid_email, None
        except Exception:
            return False, None, "Lỗi mạng hoặc lỗi code"

    def _check_profile_live(self, target_url, profile_dir):
        if not profile_dir or not os.path.exists(profile_dir):
            return False, "Profile không tồn tại"
        driver = None
        try:
            chrome_options = Options()
            chrome_options.add_argument(f"--user-data-dir={profile_dir}")
            chrome_options.add_argument("--profile-directory=Default")
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--window-size=1280,720")
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            driver.get(target_url)
            time.sleep(8)
            current_url = driver.current_url
            if "accounts.google.com" in current_url or "ServiceLogin" in current_url:
                return False, "Yêu cầu đăng nhập lại (profile)"
            return True, None
        except Exception:
            return False, "Lỗi kiểm tra profile"
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

    def check_all_accounts(self):
        row_count = self.account_table.rowCount()
        if row_count == 0:
            QMessageBox.warning(self, "Cảnh báo", "Chưa có tài khoản nào!")
            return
        QMessageBox.information(self, "Thông báo", "Đang kiểm tra trạng thái đăng nhập của tất cả tài khoản...")
        target_url = "https://labs.google/fx/vi/tools/flow"
        for i in range(row_count):
            if i >= len(self.accounts):
                break
            acc_data = self.accounts[i]
            raw_cookie = acc_data.get("cookie", "")
            profile_dir = acc_data.get("profile_dir", "")
            self.set_account_status(i, "Đang kiểm tra...", "#555555", False)
            QApplication.processEvents()
            live = False
            email = None
            error_text = None
            if raw_cookie:
                live, email, error_text = self._check_cookie_live(target_url, raw_cookie)
            elif profile_dir:
                live, error_text = self._check_profile_live(target_url, profile_dir)
            else:
                error_text = "Chưa có Cookie/Profile"
            if live:
                email_item = self.account_table.item(i, 0)
                current_email = email_item.text() if email_item else ""
                final_email = email if email else current_email
                if final_email:
                    self.account_table.setItem(i, 0, QTableWidgetItem(final_email))
                self.set_account_status(i, "LIVE (Đã đăng nhập) ✅", "green", True)
            else:
                if not error_text:
                    error_text = "Yêu cầu đăng nhập lại"
                self.set_account_status(i, error_text, "red", False)
            time.sleep(0.3)
        QMessageBox.information(self, "Hoàn tất", "Đã kiểm tra xong trạng thái các tài khoản.")
# --- KẾT THÚC SỬA LOGIC LOGIN ---

class ImageGenerationTab(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(16)
        
        # Header
        header_layout = QHBoxLayout()
        header_icon = QLabel("🖼️")
        header_icon.setStyleSheet("font-size: 24px;")
        header_title = QLabel("Tạo Ảnh AI")
        header_title.setStyleSheet("font-size: 18px; font-weight: bold; color: #1e293b;")
        header_layout.addWidget(header_icon)
        header_layout.addWidget(header_title)
        header_layout.addStretch()
        layout.addLayout(header_layout)
        
        # Settings Card
        settings_card = QFrame()
        settings_card.setObjectName("settingsCard")
        settings_card.setStyleSheet("""
            #settingsCard {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #fafafa, stop:1 #f5f5f5);
                border: 1px solid #e5e7eb;
                border-radius: 12px;
                padding: 12px;
            }
        """)
        settings_layout = QGridLayout(settings_card)
        settings_layout.setSpacing(12)
        
        # Model selection
        model_label = QLabel("🤖 Mô hình:")
        model_label.setStyleSheet("font-weight: 600; color: #475569;")
        settings_layout.addWidget(model_label, 0, 0)
        self.model_combo = QComboBox()
        self.model_combo.addItems([m["app_label"] for m in FLOW_CONFIG["image_models"]])
        settings_layout.addWidget(self.model_combo, 0, 1)
        
        # Ratio selection
        ratio_label = QLabel("📐 Tỷ lệ:")
        ratio_label.setStyleSheet("font-weight: 600; color: #475569;")
        settings_layout.addWidget(ratio_label, 0, 2)
        self.ratio_combo = QComboBox()
        self.ratio_combo.addItems([r["app_label"] for r in FLOW_CONFIG["aspect_ratios"]])
        settings_layout.addWidget(self.ratio_combo, 0, 3)
        
        # Count selection
        count_label = QLabel("🔢 Số lượng:")
        count_label.setStyleSheet("font-weight: 600; color: #475569;")
        settings_layout.addWidget(count_label, 1, 0)
        self.count_spin = QSpinBox()
        min_count = min(FLOW_CONFIG["output_counts"]["values"])
        max_count = max(FLOW_CONFIG["output_counts"]["values"])
        self.count_spin.setMinimum(min_count)
        self.count_spin.setMaximum(max_count)
        self.count_spin.setValue(min_count)
        settings_layout.addWidget(self.count_spin, 1, 1)
        
        layout.addWidget(settings_card)
        
        # Prompt Section
        prompt_header = QHBoxLayout()
        prompt_icon = QLabel("✏️")
        prompt_icon.setStyleSheet("font-size: 16px;")
        prompt_label = QLabel("Nhập Prompt")
        prompt_label.setStyleSheet("font-size: 14px; font-weight: bold; color: #1e293b;")
        prompt_header.addWidget(prompt_icon)
        prompt_header.addWidget(prompt_label)
        prompt_header.addStretch()
        layout.addLayout(prompt_header)
        
        self.prompt_text = QTextEdit()
        self.prompt_text.setPlaceholderText("Nhập prompt tại đây, mỗi dòng một prompt...\n\nVí dụ:\nA majestic lion in sunset, cinematic lighting\nCyberpunk city at night, neon lights")
        self.prompt_text.setMinimumHeight(150)
        self.prompt_text.textChanged.connect(self.update_prompt_count)
        layout.addWidget(self.prompt_text)

        # Prompt count badge
        count_layout = QHBoxLayout()
        self.prompt_count_label = QLabel("📝 0 prompt")
        self.prompt_count_label.setStyleSheet("""
            background-color: #eef2ff;
            color: #6366f1;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
        """)
        count_layout.addWidget(self.prompt_count_label)
        count_layout.addStretch()
        layout.addLayout(count_layout)
        
        # Action Buttons
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(10)
        
        self.add_queue_btn = QPushButton("➕ Thêm vào Bảng")
        self.add_queue_btn.setObjectName("primaryButton")
        self.add_queue_btn.setCursor(Qt.PointingHandCursor)
        self.add_queue_btn.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #f59e0b, stop:1 #d97706);
                color: white;
                border: none;
                border-radius: 10px;
                padding: 12px 24px;
                font-size: 13px;
                font-weight: 700;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #d97706, stop:1 #b45309);
            }
        """)
        btn_layout.addWidget(self.add_queue_btn)
        
        self.import_btn = QPushButton("📂 Nhập từ File")
        self.import_btn.setCursor(Qt.PointingHandCursor)
        self.import_btn.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #3b82f6, stop:1 #2563eb);
                color: white;
                border: none;
                border-radius: 10px;
                padding: 12px 24px;
                font-size: 13px;
                font-weight: 600;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #2563eb, stop:1 #1d4ed8);
            }
        """)
        self.import_btn.clicked.connect(self.import_prompts)
        btn_layout.addWidget(self.import_btn)
        
        self.clear_btn = QPushButton("🗑️ Xóa")
        self.clear_btn.setCursor(Qt.PointingHandCursor)
        self.clear_btn.setStyleSheet("""
            QPushButton {
                background-color: #f1f5f9;
                color: #64748b;
                border: 2px solid #e2e8f0;
                border-radius: 10px;
                padding: 12px 20px;
                font-size: 13px;
                font-weight: 600;
            }
            QPushButton:hover {
                background-color: #fee2e2;
                color: #dc2626;
                border-color: #fecaca;
            }
        """)
        self.clear_btn.clicked.connect(self.prompt_text.clear)
        btn_layout.addWidget(self.clear_btn)
        
        layout.addLayout(btn_layout)
        layout.addStretch()
        self.setLayout(layout)
    
    def update_prompt_count(self):
        lines = self.prompt_text.toPlainText().splitlines()
        count = len([line for line in lines if line.strip()])
        self.prompt_count_label.setText(f"📝 {count} prompt")

    def import_prompts(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chọn file Prompt",
            "",
            "Text Files (*.txt);;CSV Files (*.csv)"
        )
        if file_path:
            try:
                prompts = read_prompts_from_file(file_path)
                self.prompt_text.setText('\n\n'.join(prompts))
                self.update_prompt_count()
                prompt_count = len(prompts)
                QMessageBox.information(self, "Thành công", f"Đã nạp {prompt_count} prompt từ file")
            except Exception as e:
                QMessageBox.critical(self, "Lỗi", f"Không thể đọc file: {str(e)}")
                
    def get_prompts(self):
        text = self.prompt_text.toPlainText().strip()
        if not text: return []
        return [line.strip() for line in text.split('\n') if line.strip()]

class ReferencePromptHighlighter(QSyntaxHighlighter):
    def __init__(self, document):
        super().__init__(document)
        self.keywords = []
        fmt = QTextCharFormat()
        fmt.setBackground(QColor("#FFF9C4"))
        self._format = fmt

    def set_keywords(self, words):
        items = []
        for w in words:
            if isinstance(w, str):
                s = w.strip()
                if s:
                    items.append(s)
        self.keywords = items

    def highlightBlock(self, text):
        lower = text.lower()
        for w in self.keywords:
            wl = w.lower()
            if not wl:
                continue
            start = 0
            while True:
                idx = lower.find(wl, start)
                if idx < 0:
                    break
                self.setFormat(idx, len(w), self._format)
                start = idx + len(w)


class ReferenceImageNameDialog(QDialog):
    def __init__(self, paths, existing=None, parent=None):
        super().__init__(parent)
        self.paths = list(paths)
        self.existing = existing or {}
        self.edits = {}
        self.setWindowTitle("Đặt tên ảnh tham chiếu")
        self.resize(800, 600)

        layout = QVBoxLayout(self)
        info = QLabel("Đặt tên cho từng ảnh tham chiếu (tên dùng làm từ khóa trong prompt). Tối đa 10 ảnh.")
        info.setWordWrap(True)
        layout.addWidget(info)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        container = QWidget()
        grid = QGridLayout(container)
        grid.setContentsMargins(4, 4, 4, 4)
        grid.setSpacing(6)

        for idx, path in enumerate(self.paths):
            col = idx % 3
            row = idx // 3

            box = QWidget()
            v = QVBoxLayout(box)
            v.setContentsMargins(2, 2, 2, 2)
            v.setSpacing(4)

            img_label = QLabel()
            img_label.setAlignment(Qt.AlignCenter)
            pix = QPixmap(path)
            if not pix.isNull():
                img_label.setPixmap(pix.scaled(140, 140, Qt.KeepAspectRatio, Qt.SmoothTransformation))
            else:
                img_label.setText(os.path.basename(path))
            v.addWidget(img_label)

            name_edit = QLineEdit()
            base = os.path.splitext(os.path.basename(path))[0]
            default_name = self.existing.get(path, base)
            name_edit.setText(default_name)
            self.edits[path] = name_edit
            v.addWidget(name_edit)

            grid.addWidget(box, row, col)

        scroll.setWidget(container)
        layout.addWidget(scroll)

        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        ok_btn = QPushButton("OK")
        cancel_btn = QPushButton("Hủy")
        ok_btn.clicked.connect(self.accept)
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(ok_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

    def get_mapping(self):
        mapping = {}
        for path, edit in self.edits.items():
            name = edit.text().strip()
            if not name:
                name = os.path.splitext(os.path.basename(path))[0]
            mapping[path] = name
        return mapping

class VideoGenerationTab(QWidget):
    mode_changed = Signal(str)
    def __init__(self):
        super().__init__()
        self.manual_image_files: List[str] = []
        self.start_manual_files: List[str] = []
        self.end_manual_files: List[str] = []
        self.ref_manual_files: List[str] = []
        self.ref_folder_files: List[str] = []
        self.reference_all_paths: List[str] = []
        self.reference_name_map = {}
        self.reference_keyword_to_path = {}
        self.ref_highlighter: Optional[ReferencePromptHighlighter] = None
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(16)

        # Header
        header_layout = QHBoxLayout()
        header_icon = QLabel("🎬")
        header_icon.setStyleSheet("font-size: 24px;")
        header_title = QLabel("Tạo Video AI")
        header_title.setStyleSheet("font-size: 18px; font-weight: bold; color: #1e293b;")
        header_layout.addWidget(header_icon)
        header_layout.addWidget(header_title)
        header_layout.addStretch()
        layout.addLayout(header_layout)

        # ===== CẤU HÌNH CHUNG CHO VIDEO (Modern Card) =====
        config_group = QFrame()
        config_group.setObjectName("videoConfigCard")
        config_group.setStyleSheet("""
            #videoConfigCard {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #fafafa, stop:1 #f5f5f5);
                border: 1px solid #e5e7eb;
                border-radius: 12px;
                padding: 12px;
            }
        """)
        config_layout = QGridLayout(config_group)
        config_layout.setSpacing(12)

        # Model
        model_label = QLabel("🤖 Mô hình:")
        model_label.setStyleSheet("font-weight: 600; color: #475569;")
        config_layout.addWidget(model_label, 0, 0)
        self.model_combo = QComboBox()
        self.model_combo.addItems([m["app_label"] for m in FLOW_CONFIG["video_models"]])
        config_layout.addWidget(self.model_combo, 0, 1)

        # Ratio
        ratio_label = QLabel("📐 Tỷ lệ:")
        ratio_label.setStyleSheet("font-weight: 600; color: #475569;")
        config_layout.addWidget(ratio_label, 0, 2)
        self.ratio_combo = QComboBox()
        self.ratio_combo.addItems([r["app_label"] for r in FLOW_CONFIG["aspect_ratios"]])
        config_layout.addWidget(self.ratio_combo, 0, 3)

        # Count
        count_label = QLabel("🔢 Số lượng:")
        count_label.setStyleSheet("font-weight: 600; color: #475569;")
        config_layout.addWidget(count_label, 1, 0)
        self.video_per_prompt_spin = QSpinBox()
        min_count = min(FLOW_CONFIG["output_counts"]["values"])
        max_count = max(FLOW_CONFIG["output_counts"]["values"])
        self.video_per_prompt_spin.setMinimum(min_count)
        self.video_per_prompt_spin.setMaximum(max_count)
        self.video_per_prompt_spin.setValue(min_count)
        config_layout.addWidget(self.video_per_prompt_spin, 1, 1)

        # Jobs
        job_label = QLabel("📋 Công việc:")
        job_label.setStyleSheet("font-weight: 600; color: #475569;")
        config_layout.addWidget(job_label, 1, 2)
        self.job_spin = QSpinBox()
        self.job_spin.setMinimum(1)
        self.job_spin.setMaximum(999)
        self.job_spin.setValue(1)
        config_layout.addWidget(self.job_spin, 1, 3)

        # Delay
        delay_label = QLabel("⏱️ Delay (giây):")
        delay_label.setStyleSheet("font-weight: 600; color: #475569;")
        config_layout.addWidget(delay_label, 2, 0)
        self.delay_spin = QSpinBox()
        self.delay_spin.setMinimum(0)
        self.delay_spin.setMaximum(600)
        self.delay_spin.setValue(0)
        config_layout.addWidget(self.delay_spin, 2, 1)

        # Quality
        quality_label = QLabel("🎯 Chất lượng:")
        quality_label.setStyleSheet("font-weight: 600; color: #475569;")
        config_layout.addWidget(quality_label, 2, 2)
        self.quality_combo = QComboBox()
        self.quality_combo.addItems(["720p (mặc định)", "1080p (tốn credit)"])
        config_layout.addWidget(self.quality_combo, 2, 3)

        layout.addWidget(config_group)

        # ===== CHỌN CHẾ ĐỘ ĐẦU VÀO =====
        mode_layout = QHBoxLayout()
        mode_icon = QLabel("🎮")
        mode_icon.setStyleSheet("font-size: 16px;")
        mode_label = QLabel("Chế độ đầu vào:")
        mode_label.setStyleSheet("font-size: 14px; font-weight: bold; color: #1e293b;")
        mode_layout.addWidget(mode_icon)
        mode_layout.addWidget(mode_label)

        self.mode_combo = QComboBox()
        self.mode_combo.setMinimumWidth(220)
        self.mode_combo.addItem("📝 Văn bản → Video", "text")
        self.mode_combo.addItem("🖼️ Hình ảnh → Video", "image")
        self.mode_combo.addItem("🔀 Đầu + Cuối → Video", "start_end")
        self.mode_combo.addItem("📌 Tham chiếu → Video", "reference")
        self.mode_combo.addItem("➕ Mở rộng Video", "extend")
        self.mode_combo.currentIndexChanged.connect(self.on_mode_changed)
        mode_layout.addWidget(self.mode_combo)
        mode_layout.addStretch()
        layout.addLayout(mode_layout)

        # ===== STACK CHO TỪNG CHẾ ĐỘ =====
        self.mode_stack = QStackedLayout()

        self.mode_text_widget = self.build_text_mode_widget()
        self.mode_image_widget = self.build_image_mode_widget()
        self.mode_start_end_widget = self.build_start_end_mode_widget()
        self.mode_reference_widget = self.build_reference_mode_widget()
        self.mode_extend_widget = self.build_extend_mode_widget()

        for w in [
            self.mode_text_widget,
            self.mode_image_widget,
            self.mode_start_end_widget,
            self.mode_reference_widget,
            self.mode_extend_widget
        ]:
            self.mode_stack.addWidget(w)

        layout.addLayout(self.mode_stack)

        # ===== NÚT THÊM VÀO BẢNG TIẾN TRÌNH =====
        self.add_queue_btn = QPushButton("➕ Thêm vào Bảng Tiến Trình")
        self.add_queue_btn.setCursor(Qt.PointingHandCursor)
        self.add_queue_btn.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #f59e0b, stop:1 #d97706);
                color: white;
                border: none;
                border-radius: 12px;
                padding: 14px 28px;
                font-size: 14px;
                font-weight: 700;
                letter-spacing: 0.5px;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #d97706, stop:1 #b45309);
            }
        """)
        layout.addWidget(self.add_queue_btn)

        layout.addStretch()
        self.setLayout(layout)

    # =====================================================================
    #  CHẾ ĐỘ 1: VĂN BẢN → VIDEO  (CÓ MÀU NÚT IMPORT + XÓA)
    # =====================================================================
    def build_text_mode_widget(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout(w)
        v.setContentsMargins(0, 8, 0, 0)
        v.setSpacing(12)

        label = QLabel("✏️ Prompt (mỗi dòng 1 prompt video):")
        label.setStyleSheet("font-size: 13px; font-weight: 600; color: #475569;")
        v.addWidget(label)

        self.text_prompt_edit = QTextEdit()
        self.text_prompt_edit.setPlaceholderText("Mỗi dòng là 1 prompt video...")
        self.text_prompt_edit.textChanged.connect(self.update_text_prompt_count)
        v.addWidget(self.text_prompt_edit)

        self.text_prompt_count_label = QLabel("Số prompt: 0")
        v.addWidget(self.text_prompt_count_label)

        btn_line = QHBoxLayout()
        self.text_import_btn = QPushButton("📂 Nhập Prompt từ file (.txt/.csv)")
        self.text_import_btn.setStyleSheet(
            "background-color: #2196F3; color: white; padding: 8px; border-radius: 4px;"
        )
        self.text_import_btn.clicked.connect(self.import_text_prompts)
        btn_line.addWidget(self.text_import_btn)

        self.text_clear_btn = QPushButton("🗑️ Xóa")
        self.text_clear_btn.setStyleSheet(
            "background-color: #9E9E9E; color: white; padding: 8px; border-radius: 4px;"
        )
        self.text_clear_btn.clicked.connect(self.text_prompt_edit.clear)
        btn_line.addWidget(self.text_clear_btn)

        btn_line.addStretch()
        v.addLayout(btn_line)

        return w

    def update_text_prompt_count(self):
        lines = self.text_prompt_edit.toPlainText().splitlines()
        count = len([line for line in lines if line.strip()])
        self.text_prompt_count_label.setText(f"Số prompt: {count}")
    # =====================================================================
    #  CHẾ ĐỘ 2: ẢNH → VIDEO  (CÓ MÀU CHO CHỌN THƯ MỤC/FILE/ẢNH)
    # =====================================================================
    def build_image_mode_widget(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout(w)

        title = QLabel("Ảnh thành Video:")
        title.setFont(QFont("Arial", 10, QFont.Bold))
        v.addWidget(title)

        folder_layout = QHBoxLayout()
        folder_layout.addWidget(QLabel("Thư mục chứa ảnh (xử lý hàng loạt):"))
        self.image_folder_edit = QLineEdit()
        self.image_folder_edit.setPlaceholderText("Chưa chọn thư mục...")
        folder_layout.addWidget(self.image_folder_edit)
        browse_folder_btn = QPushButton("Chọn thư mục")
        browse_folder_btn.setStyleSheet(
            "background-color: #4CAF50; color: white; padding: 6px 10px; border-radius: 4px;"
        )
        browse_folder_btn.clicked.connect(self.choose_image_folder)
        folder_layout.addWidget(browse_folder_btn)
        v.addLayout(folder_layout)

        manual_layout = QHBoxLayout()
        manual_layout.addWidget(QLabel("Ảnh thủ công:"))
        self.image_manual_label = QLabel("Chưa chọn ảnh.")
        manual_layout.addWidget(self.image_manual_label)
        manual_btn = QPushButton("Chọn nhiều ảnh")
        manual_btn.setStyleSheet(
            "background-color: #9C27B0; color: white; padding: 6px 10px; border-radius: 4px;"
        )
        manual_btn.clicked.connect(self.select_image_files)
        manual_layout.addWidget(manual_btn)
        v.addLayout(manual_layout)

        prompt_file_layout = QHBoxLayout()
        prompt_file_layout.addWidget(QLabel("File chứa Prompt (.txt/.csv):"))
        self.image_prompt_file_edit = QLineEdit()
        self.image_prompt_file_edit.setPlaceholderText("Mỗi dòng 1 prompt, sẽ map theo thứ tự ảnh...")
        prompt_file_layout.addWidget(self.image_prompt_file_edit)
        browse_prompt_file_btn = QPushButton("Chọn file")
        browse_prompt_file_btn.setStyleSheet(
            "background-color: #2196F3; color: white; padding: 6px 10px; border-radius: 4px;"
        )
        browse_prompt_file_btn.clicked.connect(self.choose_image_prompt_file)
        prompt_file_layout.addWidget(browse_prompt_file_btn)
        v.addLayout(prompt_file_layout)

        note = QLabel("Nếu số prompt < số ảnh → prompt cuối cùng sẽ dùng cho các ảnh còn lại.")
        note.setWordWrap(True)
        v.addWidget(note)

        return w

    # =====================================================================
    #  CHẾ ĐỘ 3: ĐẦU + CUỐI → VIDEO  (START/END MODE)
    # =====================================================================
    def build_start_end_mode_widget(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout(w)

        title = QLabel("Đầu + cuối thành Video:")
        title.setFont(QFont("Arial", 10, QFont.Bold))
        v.addWidget(title)

        start_folder_layout = QHBoxLayout()
        start_folder_layout.addWidget(QLabel("Thư mục ảnh ĐẦU:"))
        self.start_folder_edit = QLineEdit()
        self.start_folder_edit.setPlaceholderText("Chưa chọn thư mục ảnh đầu...")
        start_folder_layout.addWidget(self.start_folder_edit)
        start_folder_btn = QPushButton("Chọn thư mục")
        start_folder_btn.setStyleSheet(
            "background-color: #4CAF50; color: white; padding: 6px 10px; border-radius: 4px;"
        )
        start_folder_btn.clicked.connect(self.choose_start_folder)
        start_folder_layout.addWidget(start_folder_btn)
        v.addLayout(start_folder_layout)

        end_folder_layout = QHBoxLayout()
        end_folder_layout.addWidget(QLabel("Thư mục ảnh CUỐI:"))
        self.end_folder_edit = QLineEdit()
        self.end_folder_edit.setPlaceholderText("Chưa chọn thư mục ảnh cuối...")
        end_folder_layout.addWidget(self.end_folder_edit)
        end_folder_btn = QPushButton("Chọn thư mục")
        end_folder_btn.setStyleSheet(
            "background-color: #4CAF50; color: white; padding: 6px 10px; border-radius: 4px;"
        )
        end_folder_btn.clicked.connect(self.choose_end_folder)
        end_folder_layout.addWidget(end_folder_btn)
        v.addLayout(end_folder_layout)

        manual_start_layout = QHBoxLayout()
        manual_start_layout.addWidget(QLabel("Ảnh ĐẦU (chọn tay):"))
        self.start_manual_label = QLabel("Chưa chọn ảnh đầu.")
        manual_start_layout.addWidget(self.start_manual_label)
        start_manual_btn = QPushButton("Chọn nhiều ảnh đầu")
        start_manual_btn.setStyleSheet(
            "background-color: #9C27B0; color: white; padding: 6px 10px; border-radius: 4px;"
        )
        start_manual_btn.clicked.connect(self.select_start_files)
        manual_start_layout.addWidget(start_manual_btn)
        v.addLayout(manual_start_layout)

        manual_end_layout = QHBoxLayout()
        manual_end_layout.addWidget(QLabel("Ảnh CUỐI (chọn tay):"))
        self.end_manual_label = QLabel("Chưa chọn ảnh cuối.")
        manual_end_layout.addWidget(self.end_manual_label)
        end_manual_btn = QPushButton("Chọn nhiều ảnh cuối")
        end_manual_btn.setStyleSheet(
            "background-color: #9C27B0; color: white; padding: 6px 10px; border-radius: 4px;"
        )
        end_manual_btn.clicked.connect(self.select_end_files)
        manual_end_layout.addWidget(end_manual_btn)
        v.addLayout(manual_end_layout)

        prompt_file_layout = QHBoxLayout()
        prompt_file_layout.addWidget(QLabel("File Prompt (.txt/.csv):"))
        self.start_end_prompt_file_edit = QLineEdit()
        self.start_end_prompt_file_edit.setPlaceholderText("Mỗi dòng 1 prompt, sẽ map theo từng cặp đầu/cuối...")
        prompt_file_layout.addWidget(self.start_end_prompt_file_edit)
        start_end_prompt_btn = QPushButton("Chọn file")
        start_end_prompt_btn.setStyleSheet(
            "background-color: #2196F3; color: white; padding: 6px 10px; border-radius: 4px;"
        )
        start_end_prompt_btn.clicked.connect(self.choose_start_end_prompt_file)
        prompt_file_layout.addWidget(start_end_prompt_btn)
        v.addLayout(prompt_file_layout)

        note = QLabel(
            "Ảnh ĐẦU và CUỐI sẽ được ghép theo thứ tự (index 1 với 1, 2 với 2,...).\n"
            "Nếu số prompt ít hơn số cặp ảnh → prompt cuối cùng dùng cho các cặp còn lại."
        )
        note.setWordWrap(True)
        v.addWidget(note)

        return w
    #  CHẾ ĐỘ 4: ẢNH THAM CHIẾU → VIDEO  (TỐI ĐA 3 ẢNH / 1 PROMPT)
    # =====================================================================
    def build_reference_mode_widget(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout(w)

        title = QLabel("Tham chiếu thành Video:")
        title.setFont(QFont("Arial", 10, QFont.Bold))
        v.addWidget(title)

        folder_layout = QHBoxLayout()
        folder_layout.addWidget(QLabel("Thư mục ảnh tham chiếu:"))
        self.ref_image_folder_edit = QLineEdit()
        self.ref_image_folder_edit.setPlaceholderText("Chưa chọn thư mục ảnh tham chiếu...")
        folder_layout.addWidget(self.ref_image_folder_edit)
        ref_folder_btn = QPushButton("Chọn thư mục")
        ref_folder_btn.setStyleSheet(
            "background-color: #4CAF50; color: white; padding: 6px 10px; border-radius: 4px;"
        )
        ref_folder_btn.clicked.connect(self.choose_ref_image_folder)
        folder_layout.addWidget(ref_folder_btn)
        v.addLayout(folder_layout)

        manual_layout = QHBoxLayout()
        manual_layout.addWidget(QLabel("Ảnh tham chiếu (chọn tay):"))
        self.ref_manual_label = QLabel("Chưa chọn ảnh tham chiếu.")
        manual_layout.addWidget(self.ref_manual_label)
        ref_manual_btn = QPushButton("Chọn nhiều ảnh")
        ref_manual_btn.setStyleSheet(
            "background-color: #9C27B0; color: white; padding: 6px 10px; border-radius: 4px;"
        )
        ref_manual_btn.clicked.connect(self.select_ref_image_files)
        manual_layout.addWidget(ref_manual_btn)
        v.addLayout(manual_layout)

        group_layout = QHBoxLayout()
        group_layout.addWidget(QLabel("Số ảnh tối đa / 1 video (1-3):"))
        self.ref_group_spin = QSpinBox()
        self.ref_group_spin.setMinimum(1)
        self.ref_group_spin.setMaximum(3)
        self.ref_group_spin.setValue(3)
        group_layout.addWidget(self.ref_group_spin)
        group_layout.addStretch()
        v.addLayout(group_layout)

        prompt_label = QLabel("Prompt tham chiếu (mỗi dòng 1 prompt):")
        prompt_label.setFont(QFont("Arial", 9, QFont.Bold))
        v.addWidget(prompt_label)

        self.ref_prompt_edit = QTextEdit()
        self.ref_prompt_edit.setPlaceholderText("Mỗi dòng 1 prompt dùng ảnh tham chiếu...")
        v.addWidget(self.ref_prompt_edit)

        prompt_file_layout = QHBoxLayout()
        prompt_file_layout.addWidget(QLabel("File Prompt (.txt/.csv):"))
        self.ref_prompt_file_edit = QLineEdit()
        self.ref_prompt_file_edit.setPlaceholderText("Có thể chọn file để nạp prompt, sau đó chỉnh trong ô phía trên.")
        prompt_file_layout.addWidget(self.ref_prompt_file_edit)
        ref_prompt_btn = QPushButton("Chọn file")
        ref_prompt_btn.setStyleSheet(
            "background-color: #2196F3; color: white; padding: 6px 10px; border-radius: 4px;"
        )
        ref_prompt_btn.clicked.connect(self.choose_ref_prompt_file)
        prompt_file_layout.addWidget(ref_prompt_btn)
        v.addLayout(prompt_file_layout)

        note = QLabel(
            "Tool sẽ tự nhận diện từ khóa khớp với tên ảnh tham chiếu trong prompt.\n"
            "Mỗi video dùng tối đa 3 ảnh tham chiếu khớp từ khóa."
        )
        note.setWordWrap(True)
        v.addWidget(note)

        self.ref_highlighter = ReferencePromptHighlighter(self.ref_prompt_edit.document())

        return w

    #  CHẾ ĐỘ 5: MỞ RỘNG VIDEO  (GIỮ INFO NHƯ CŨ, CHƯA TỰ ĐỘNG)
    # =====================================================================
    def build_extend_mode_widget(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout(w)
        info = QLabel(
            "Chế độ Mở rộng Video:\n"
            "- Dùng để mở rộng video có sẵn (extend).\n"
            "- Chưa được tự động hóa trong phiên bản này để tránh lỗi ngoài ý muốn.\n"
            "- Bạn có thể mô tả yêu cầu extend bằng prompt ở chế độ Văn bản thành Video."
        )
        info.setWordWrap(True)
        v.addWidget(info)
        return w

    def on_mode_changed(self, index: int):
        self.mode_stack.setCurrentIndex(index)
        data = self.mode_combo.itemData(index)
        mode_code = str(data) if data is not None else ""
        self.mode_changed.emit(mode_code)

    def import_text_prompts(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chọn file Prompt",
            "",
            "Text/CSV (*.txt *.csv)"
        )
        if not file_path:
            return
        prompts = read_prompts_from_file(file_path)
        if not prompts:
            QMessageBox.warning(self, "Lỗi", "Không đọc được prompt hợp lệ từ file.")
            return
        self.text_prompt_edit.setPlainText("\n\n".join(prompts))
        self.update_text_prompt_count()
        QMessageBox.information(self, "Thành công", f"Đã nạp {len(prompts)} prompt từ file.")

    def choose_image_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Chọn thư mục chứa ảnh")
        if folder:
            self.image_folder_edit.setText(folder)

    def select_image_files(self):
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Chọn nhiều ảnh",
            "",
            "Images (*.png *.jpg *.jpeg *.webp *.gif)"
        )
        if files:
            self.manual_image_files = files
            self.image_manual_label.setText(f"Đã chọn {len(files)} ảnh thủ công.")

    def choose_image_prompt_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chọn file Prompt cho ảnh",
            "",
            "Text/CSV (*.txt *.csv)"
        )
        if file_path:
            self.image_prompt_file_edit.setText(file_path)

    def choose_start_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Chọn thư mục ảnh ĐẦU")
        if folder:
            self.start_folder_edit.setText(folder)

    def choose_end_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Chọn thư mục ảnh CUỐI")
        if folder:
            self.end_folder_edit.setText(folder)

    def select_start_files(self):
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Chọn nhiều ảnh ĐẦU",
            "",
            "Images (*.png *.jpg *.jpeg *.webp *.gif)"
        )
        if files:
            self.start_manual_files = files
            self.start_manual_label.setText(f"Đã chọn {len(files)} ảnh đầu.")

    def select_end_files(self):
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Chọn nhiều ảnh CUỐI",
            "",
            "Images (*.png *.jpg *.jpeg *.webp *.gif)"
        )
        if files:
            self.end_manual_files = files
            self.end_manual_label.setText(f"Đã chọn {len(files)} ảnh cuối.")

    def choose_start_end_prompt_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chọn file Prompt cho đầu/cuối",
            "",
            "Text/CSV (*.txt *.csv)"
        )
        if file_path:
            self.start_end_prompt_file_edit.setText(file_path)

    def choose_ref_image_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Chọn thư mục ảnh tham chiếu")
        if not folder:
            return
        self.ref_image_folder_edit.setText(folder)
        paths = []
        for name in os.listdir(folder):
            if name.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".heic", ".avif")):
                paths.append(os.path.join(folder, name))
        self.ref_folder_files = paths
        self.build_reference_image_list()

    def select_ref_image_files(self):
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Chọn nhiều ảnh tham chiếu",
            "",
            "Images (*.png *.jpg *.jpeg *.webp *.gif *.heic *.avif)"
        )
        if not files:
            return
        self.ref_manual_files = files
        self.build_reference_image_list()

    def build_reference_image_list(self):
        all_paths = []
        for p in self.ref_folder_files:
            all_paths.append(os.path.abspath(p))
        for p in self.ref_manual_files:
            all_paths.append(os.path.abspath(p))
        uniq = []
        seen = set()
        for p in all_paths:
            if p not in seen:
                seen.add(p)
                uniq.append(p)
        if not uniq:
            self.reference_all_paths = []
            self.reference_name_map = {}
            self.reference_keyword_to_path = {}
            self.update_reference_keywords()
            self.ref_manual_label.setText("Chưa chọn ảnh tham chiếu.")
            return
        uniq = uniq[:10]
        existing = {}
        for p in uniq:
            existing[p] = self.reference_name_map.get(p, "")
        dlg = ReferenceImageNameDialog(uniq, existing, self)
        if dlg.exec() == QDialog.Accepted:
            mapping = dlg.get_mapping()
        else:
            mapping = {}
            for p in uniq:
                base = os.path.splitext(os.path.basename(p))[0]
                val = existing.get(p, base)
                if not val:
                    val = base
                mapping[p] = val
        self.reference_all_paths = uniq
        self.reference_name_map = mapping
        kw_to_path = {}
        for path, name in mapping.items():
            key = (name or "").strip()
            if not key:
                key = os.path.splitext(os.path.basename(path))[0]
            kw_to_path[key] = path
        self.reference_keyword_to_path = kw_to_path
        self.update_reference_keywords()
        self.ref_manual_label.setText(f"Đã chọn {len(self.reference_all_paths)} ảnh tham chiếu.")

    def update_reference_keywords(self):
        if not self.ref_highlighter:
            return
        words = list(self.reference_keyword_to_path.keys())
        self.ref_highlighter.set_keywords(words)

    def choose_ref_prompt_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Chọn file Prompt cho tham chiếu",
            "",
            "Text/CSV (*.txt *.csv)"
        )
        if not file_path:
            return
        self.ref_prompt_file_edit.setText(file_path)
        prompts = read_prompts_from_file(file_path)
        if prompts:
            self.ref_prompt_edit.setPlainText("\n".join(prompts))

    def get_mode_display_label(self, mode_code: str) -> str:
        mapping = {
            "text": "text -> video",
            "image": "image -> video",
            "start_end": "start-end video",
            "reference": "tạo video tham chiếu",
            "extend": "Mở rộng video"
        }
        return mapping.get(str(mode_code), "")
    #  HÀNG CHỜ TASK CHO TOÀN BỘ MODE (text / image / start_end / reference)
    # =====================================================================
    def collect_tasks(self) -> List[Dict]:
        mode_code = self.mode_combo.currentData()
        tasks: List[Dict] = []
        video_count = self.video_per_prompt_spin.value()

        if mode_code == "text":
            raw = self.text_prompt_edit.toPlainText().strip()
            if not raw:
                QMessageBox.warning(self, "Thiếu Prompt", "Vui lòng nhập ít nhất 1 prompt (mỗi dòng 1 prompt).")
                return []
            lines = [line.strip() for line in raw.splitlines() if line.strip()]
            for line in lines:
                tasks.append({
                    "prompt": line,
                    "count": video_count,
                    "mode": mode_code
                })
            return tasks

        if mode_code == "image":
            image_paths: List[str] = []

            folder = self.image_folder_edit.text().strip()
            if folder and os.path.isdir(folder):
                for name in os.listdir(folder):
                    if name.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".heic", ".avif")):
                        image_paths.append(os.path.join(folder, name))

            if self.manual_image_files:
                image_paths.extend(self.manual_image_files)

            uniq = []
            seen = set()
            for p in image_paths:
                p = os.path.abspath(p)
                if p not in seen:
                    seen.add(p)
                    uniq.append(p)
            image_paths = uniq

            if not image_paths:
                QMessageBox.warning(self, "Thiếu ảnh", "Vui lòng chọn thư mục ảnh hoặc chọn ít nhất 1 file ảnh.")
                return []

            prompts_file = self.image_prompt_file_edit.text().strip()
            prompts_list: List[str] = read_prompts_from_file(prompts_file) if prompts_file else []

            for idx, path in enumerate(image_paths):
                base_prompt = ""
                if prompts_list:
                    if idx < len(prompts_list):
                        base_prompt = prompts_list[idx]
                    else:
                        base_prompt = prompts_list[-1]

                prefix = f"Img2Vid: {os.path.basename(path)}"
                if base_prompt:
                    final_prompt = f"{prefix} | {base_prompt}"
                else:
                    final_prompt = prefix

                tasks.append({
                    "prompt": final_prompt,
                    "count": video_count,
                    "mode": mode_code,
                    "image_path": path
                })

            return tasks

        if mode_code == "start_end":
            start_paths: List[str] = []
            end_paths: List[str] = []

            start_folder = self.start_folder_edit.text().strip()
            if start_folder and os.path.isdir(start_folder):
                for name in os.listdir(start_folder):
                    if name.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".heic", ".avif")):
                        start_paths.append(os.path.join(start_folder, name))

            if self.start_manual_files:
                start_paths.extend(self.start_manual_files)

            end_folder = self.end_folder_edit.text().strip()
            if end_folder and os.path.isdir(end_folder):
                for name in os.listdir(end_folder):
                    if name.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".heic", ".avif")):
                        end_paths.append(os.path.join(end_folder, name))

            if self.end_manual_files:
                end_paths.extend(self.end_manual_files)

            def _uniq(lst):
                out = []
                seen = set()
                for p in lst:
                    p = os.path.abspath(p)
                    if p not in seen:
                        seen.add(p)
                        out.append(p)
                return out

            start_paths = _uniq(start_paths)
            end_paths = _uniq(end_paths)

            pair_count = min(len(start_paths), len(end_paths))

            if pair_count == 0:
                QMessageBox.warning(
                    self,
                    "Thiếu ảnh",
                    "Cần ít nhất 1 cặp ảnh ĐẦU và CUỐI.\nVui lòng kiểm tra lại thư mục/ảnh đã chọn."
                )
                return []

            prompts_file = self.start_end_prompt_file_edit.text().strip()
            prompts_list: List[str] = read_prompts_from_file(prompts_file) if prompts_file else []

            for idx in range(pair_count):
                start_file = start_paths[idx]
                end_file = end_paths[idx]

                base_prompt = ""
                if prompts_list:
                    if idx < len(prompts_list):
                        base_prompt = prompts_list[idx]
                    else:
                        base_prompt = prompts_list[-1]

                prefix = f"Img2Vid: StartEnd[{os.path.basename(start_file)} -> {os.path.basename(end_file)}]"
                if base_prompt:
                    final_prompt = f"{prefix} | {base_prompt}"
                else:
                    final_prompt = prefix

                tasks.append({
                    "prompt": final_prompt,
                    "count": video_count,
                    "mode": mode_code,
                    "start_image": start_file,
                    "end_image": end_file
                })

            if len(start_paths) != len(end_paths):
                QMessageBox.information(
                    self,
                    "Lưu ý",
                    f"Đã tạo {pair_count} cặp đầu/cuối.\nMột số ảnh dư không được dùng vì số lượng ĐẦU và CUỐI không bằng nhau."
                )

            return tasks

        if mode_code == "reference":
            if not self.reference_all_paths:
                self.build_reference_image_list()

            image_paths = list(self.reference_all_paths)
            if not image_paths:
                QMessageBox.warning(
                    self,
                    "Thiếu ảnh tham chiếu",
                    "Vui lòng chọn thư mục ảnh hoặc chọn ít nhất 1 file ảnh tham chiếu."
                )
                return []

            text_raw = self.ref_prompt_edit.toPlainText().strip()
            lines = [line.strip() for line in text_raw.splitlines() if line.strip()]

            if not lines:
                prompts_file = self.ref_prompt_file_edit.text().strip()
                if prompts_file:
                    prompts_list = read_prompts_from_file(prompts_file)
                    lines = [p.strip() for p in prompts_list if p.strip()]

            if not lines:
                QMessageBox.warning(
                    self,
                    "Thiếu Prompt",
                    "Vui lòng nhập ít nhất 1 prompt tham chiếu (mỗi dòng 1 prompt) hoặc chọn file prompt."
                )
                return []

            if not self.reference_keyword_to_path:
                kw_to_path = {}
                for p in image_paths:
                    base = os.path.splitext(os.path.basename(p))[0]
                    kw_to_path[base] = p
                self.reference_keyword_to_path = kw_to_path
                self.update_reference_keywords()

            group_size = max(1, min(3, int(self.ref_group_spin.value())))
            names_sorted = sorted(self.reference_keyword_to_path.keys(), key=lambda s: len(s), reverse=True)

            for prompt in lines:
                lower = prompt.lower()
                used_paths: List[str] = []
                used_names: List[str] = []
                for name in names_sorted:
                    n = name.strip()
                    if not n:
                        continue
                    if n.lower() in lower:
                        path = self.reference_keyword_to_path.get(name)
                        if path and path not in used_paths:
                            used_paths.append(path)
                            used_names.append(name)
                    if len(used_paths) >= group_size:
                        break

                if not used_paths:
                    used_paths = image_paths[:group_size]
                    used_names = []
                    for p in used_paths:
                        nm = self.reference_name_map.get(p)
                        if not nm:
                            nm = os.path.splitext(os.path.basename(p))[0]
                        used_names.append(nm)

                tasks.append({
                    "prompt": prompt,
                    "count": video_count,
                    "mode": mode_code,
                    "ref_images": used_paths,
                    "ref_keywords": used_names
                })

            return tasks

        QMessageBox.information(
            self,
            "Chưa hỗ trợ tự động",
            "Chế độ này hiện mới có giao diện, chưa được đưa vào hàng chờ tự động.\nBạn hãy dùng các chế độ khác để chạy tool."
        )
        return []

        # ---- 1. VĂN BẢN → VIDEO ----
        if mode_code == "text":
            raw = self.text_prompt_edit.toPlainText().strip()
            if not raw:
                QMessageBox.warning(self, "Thiếu Prompt", "Vui lòng nhập ít nhất 1 prompt (mỗi dòng 1 prompt).")
                return []
            lines = [line.strip() for line in raw.splitlines() if line.strip()]
            for line in lines:
                tasks.append({
                    "prompt": line,
                    "count": video_count,
                    "mode": mode_code
                })
            return tasks

        # ---- 2. ẢNH → VIDEO (Img2Vid) ----
        if mode_code == "image":
            image_paths: List[str] = []

            folder = self.image_folder_edit.text().strip()
            if folder and os.path.isdir(folder):
                for name in os.listdir(folder):
                    if name.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
                        image_paths.append(os.path.join(folder, name))

            if self.manual_image_files:
                image_paths.extend(self.manual_image_files)

            image_paths = sorted(dict.fromkeys(image_paths))

            if not image_paths:
                QMessageBox.warning(self, "Thiếu ảnh", "Vui lòng chọn thư mục ảnh hoặc chọn ít nhất 1 file ảnh.")
                return []

            prompts_file = self.image_prompt_file_edit.text().strip()
            prompts_list: List[str] = read_prompts_from_file(prompts_file) if prompts_file else []

            for idx, path in enumerate(image_paths):
                base_prompt = ""
                if prompts_list:
                    if idx < len(prompts_list):
                        base_prompt = prompts_list[idx]
                    else:
                        base_prompt = prompts_list[-1]

                prefix = f"Img2Vid: {os.path.basename(path)}"
                if base_prompt:
                    final_prompt = f"{prefix} | {base_prompt}"
                else:
                    final_prompt = prefix

                tasks.append({
                    "prompt": final_prompt,
                    "count": video_count,
                    "mode": mode_code
                })

            return tasks

        # ---- 3. ĐẦU + CUỐI → VIDEO (cặp ảnh) ----
        if mode_code == "start_end":
            start_paths: List[str] = []
            end_paths: List[str] = []

            start_folder = self.start_folder_edit.text().strip()
            if start_folder and os.path.isdir(start_folder):
                for name in os.listdir(start_folder):
                    if name.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
                        start_paths.append(os.path.join(start_folder, name))

            if self.start_manual_files:
                start_paths.extend(self.start_manual_files)

            end_folder = self.end_folder_edit.text().strip()
            if end_folder and os.path.isdir(end_folder):
                for name in os.listdir(end_folder):
                    if name.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
                        end_paths.append(os.path.join(end_folder, name))

            if self.end_manual_files:
                end_paths.extend(self.end_manual_files)

            start_paths = sorted(dict.fromkeys(start_paths))
            end_paths = sorted(dict.fromkeys(end_paths))

            pair_count = min(len(start_paths), len(end_paths))

            if pair_count == 0:
                QMessageBox.warning(
                    self,
                    "Thiếu ảnh",
                    "Cần ít nhất 1 cặp ảnh ĐẦU và CUỐI.\n"
                    "Vui lòng kiểm tra lại thư mục/ảnh đã chọn."
                )
                return []

            prompts_file = self.start_end_prompt_file_edit.text().strip()
            prompts_list: List[str] = read_prompts_from_file(prompts_file) if prompts_file else []

            for idx in range(pair_count):
                start_file = start_paths[idx]
                end_file = end_paths[idx]

                base_prompt = ""
                if prompts_list:
                    if idx < len(prompts_list):
                        base_prompt = prompts_list[idx]
                    else:
                        base_prompt = prompts_list[-1]

                prefix = f"Img2Vid: StartEnd[{os.path.basename(start_file)} -> {os.path.basename(end_file)}]"
                if base_prompt:
                    final_prompt = f"{prefix} | {base_prompt}"
                else:
                    final_prompt = prefix

                tasks.append({
                    "prompt": final_prompt,
                    "count": video_count,
                    "mode": mode_code
                })

            if len(start_paths) != len(end_paths):
                QMessageBox.information(
                    self,
                    "Lưu ý",
                    f"Đã tạo {pair_count} cặp đầu/cuối.\n"
                    "Một số ảnh dư không được dùng vì số lượng ĐẦU và CUỐI không bằng nhau."
                )

            return tasks

        # ---- 4. ẢNH THAM CHIẾU → VIDEO (tối đa 3 ảnh / prompt) ----
        if mode_code == "reference":
            image_paths: List[str] = []

            folder = self.ref_image_folder_edit.text().strip()
            if folder and os.path.isdir(folder):
                for name in os.listdir(folder):
                    if name.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
                        image_paths.append(os.path.join(folder, name))

            if self.ref_manual_files:
                image_paths.extend(self.ref_manual_files)

            image_paths = sorted(dict.fromkeys(image_paths))

            if not image_paths:
                QMessageBox.warning(
                    self,
                    "Thiếu ảnh tham chiếu",
                    "Vui lòng chọn thư mục ảnh hoặc chọn ít nhất 1 file ảnh tham chiếu."
                )
                return []

            group_size = self.ref_group_spin.value()
            groups: List[List[str]] = []
            for i in range(0, len(image_paths), group_size):
                groups.append(image_paths[i:i + group_size])

            prompts_file = self.ref_prompt_file_edit.text().strip()
            prompts_list: List[str] = read_prompts_from_file(prompts_file) if prompts_file else []

            for g_idx, group in enumerate(groups):
                base_prompt = ""
                if prompts_list:
                    if g_idx < len(prompts_list):
                        base_prompt = prompts_list[g_idx]
                    else:
                        base_prompt = prompts_list[-1]

                img_names = ", ".join(os.path.basename(p) for p in group)
                prefix = f"Img2Vid: Ref[{img_names}]"
                if base_prompt:
                    final_prompt = f"{prefix} | {base_prompt}"
                else:
                    final_prompt = prefix

                tasks.append({
                    "prompt": final_prompt,
                    "count": video_count,
                    "mode": mode_code
                })

            return tasks

        # ---- 5. MODE KHÁC (extend) CHƯA TỰ ĐỘNG HÓA ----
        QMessageBox.information(
            self,
            "Chưa hỗ trợ tự động",
            "Chế độ này hiện mới có giao diện, chưa được đưa vào hàng chờ tự động.\n"
            "Bạn hãy dùng các chế độ khác để chạy tool."
        )
        return []

class LogWidget(QWidget):
    def __init__(self):
        super().__init__()
        self._log_entries = []
        self._last_raw_message = ""
        self._last_log_type = ""
        self._repeat_count = 0
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        
        # Header với icon và nút xóa
        header_widget = QWidget()
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(0, 0, 0, 0)

        header_icon = QLabel("📋")
        header_icon.setStyleSheet("font-size: 16px;")
        header_layout.addWidget(header_icon)
        
        title_label = QLabel("Nhật Ký Hoạt Động")
        title_label.setStyleSheet("""
            font-weight: 700; 
            font-size: 13px; 
            color: #1e293b;
            letter-spacing: 0.5px;
        """)
        header_layout.addWidget(title_label)
        
        header_layout.addStretch()
        
        clear_btn = QPushButton("🗑️ Xóa")
        clear_btn.setCursor(Qt.PointingHandCursor)
        clear_btn.setStyleSheet("""
            QPushButton {
                border: 2px solid #e2e8f0;
                color: #64748b;
                font-size: 11px;
                padding: 4px 12px;
                background: #f8fafc;
                border-radius: 6px;
                font-weight: 600;
            }
            QPushButton:hover {
                color: #dc2626;
                background-color: #fee2e2;
                border-color: #fecaca;
            }
        """)
        clear_btn.clicked.connect(self.clear_logs)
        header_layout.addWidget(clear_btn)
        
        layout.addWidget(header_widget)
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(200)
        # Modern log styling
        self.log_text.setStyleSheet("""
            QTextEdit {
                background-color: #fafafa;
                border: 2px solid #e2e8f0;
                border-radius: 10px;
                padding: 8px;
                font-family: 'Segoe UI', 'SF Pro Display', sans-serif;
                font-size: 12px;
            }
        """)
        layout.addWidget(self.log_text)
        
        self.setLayout(layout)
        
    def add_log(self, message, log_type="info"):
        timestamp = datetime.now().strftime("%H:%M:%S")

        styles = {
            "info":     {"color": "#0077B6", "icon": "ℹ️",  "bg": "#E0F7FA"},
            "process":  {"color": "#D35400", "icon": "⚙️",  "bg": "#FDEBD0"},
            "success":  {"color": "#27AE60", "icon": "✅",  "bg": "#EAFAF1"},
            "warning":  {"color": "#F39C12", "icon": "⚠️",  "bg": "#FEF9E7"},
            "error":    {"color": "#C0392B", "icon": "❌",  "bg": "#F9EBEA"},
            "download": {"color": "#8E44AD", "icon": "⬇️",  "bg": "#F4ECF7"},
            "network":  {"color": "#16A085", "icon": "🌐",  "bg": "#E8F8F5"},
            "save":     {"color": "#2C3E50", "icon": "💾",  "bg": "#EBEDEF"}
        }

        style = styles.get(log_type, styles["info"])

        is_repeat = (
            self._log_entries
            and message == self._last_raw_message
            and log_type == self._last_log_type
        )

        if is_repeat:
            self._repeat_count += 1
            display_message = f"{message} (lần {self._repeat_count})"
            entry_index = len(self._log_entries) - 1
        else:
            self._repeat_count = 1
            self._last_raw_message = message
            self._last_log_type = log_type
            display_message = message
            entry_index = None

        formatted_message = (
            f'<div style="background-color: {style["bg"]}; margin-bottom: 3px; padding: 5px 8px; border-radius: 5px; border-left: 3px solid {style["color"]};">'
            f'<span style="color: #7F8C8D; font-size: 10px; margin-right: 8px; font-family: Consolas;">[{timestamp}]</span>'
            f'<span style="font-size: 14px; margin-right: 6px;">{style["icon"]}</span>'
            f'<span style="color: {style["color"]}; font-weight: 600; font-family: Segoe UI;">{display_message}</span>'
            f'</div>'
        )

        if is_repeat and entry_index is not None:
            self._log_entries[entry_index]["html"] = formatted_message
        else:
            self._log_entries.append({
                "html": formatted_message,
                "raw_message": message,
                "log_type": log_type
            })

        full_html = "".join(entry["html"] for entry in self._log_entries)
        self.log_text.setHtml(full_html)
        self.log_text.verticalScrollBar().setValue(self.log_text.verticalScrollBar().maximum())
        
    def clear_logs(self):
        self.log_text.clear()
        self._log_entries = []
        self._last_raw_message = ""
        self._last_log_type = ""
        self._repeat_count = 0

class ZoomableGraphicsView(QGraphicsView):
    def __init__(self, pixmap, parent=None):
        super().__init__(parent)
        scene = QGraphicsScene(self)
        self._pix_item = scene.addPixmap(pixmap)
        self.setScene(scene)
        self.setRenderHints(QPainter.Antialiasing | QPainter.SmoothPixmapTransform)
        self.setDragMode(QGraphicsView.ScrollHandDrag)
        self.setTransformationAnchor(QGraphicsView.AnchorUnderMouse)
        self.setResizeAnchor(QGraphicsView.AnchorUnderMouse)
        self._current_scale = 1.0
        self.fitInView(self._pix_item, Qt.KeepAspectRatio)

    def wheelEvent(self, event):
        angle = event.angleDelta().y()
        if angle > 0:
            factor = 1.25
        else:
            factor = 0.8
        new_scale = self._current_scale * factor
        if new_scale < 0.1 or new_scale > 10.0:
            return
        self._current_scale = new_scale
        self.scale(factor, factor)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self._current_scale == 1.0:
            self.fitInView(self._pix_item, Qt.KeepAspectRatio)


class ImageZoomDialog(QDialog):
    def __init__(self, pixmap, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Xem ảnh lớn")
        self.resize(900, 700)
        layout = QVBoxLayout(self)
        view = ZoomableGraphicsView(pixmap, self)
        layout.addWidget(view)

class ResultTable(QWidget):
    request_regenerate = Signal(str, str)
    request_run_image = Signal(str)
    request_run_video = Signal(str)

    def __init__(self):
        super().__init__()
        self.tasks = {}
        self.mode = "image"
        self.progress_targets = {}
        self.smooth_timer = QTimer(self)
        self.smooth_timer.setInterval(50)
        self.smooth_timer.timeout.connect(self._smooth_progress_tick)
        self.auto_progress_max = 95
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        
        # --- KHU VỰC TOOLBAR TRÊN BẢNG (SỬA LỖI 2) ---
        toolbar = QHBoxLayout()
        
        # Checkbox chọn tất cả
        self.select_all_check = QCheckBox("Chọn tất cả")
        self.select_all_check.stateChanged.connect(self.toggle_select_all)
        toolbar.addWidget(self.select_all_check)
        
        toolbar.addStretch()
        
        # Nút xóa (Icon thùng rác)
        self.delete_selected_btn = QPushButton("🗑️ Xóa đã chọn")
        self.delete_selected_btn.setStyleSheet("""
            QPushButton {
                background-color: #f44336; 
                color: white; 
                font-weight: bold;
                padding: 6px 12px;
                border-radius: 4px;
            }
            QPushButton:hover { background-color: #d32f2f; }
        """)
        self.delete_selected_btn.clicked.connect(self.delete_selected)
        toolbar.addWidget(self.delete_selected_btn)
        
        layout.addLayout(toolbar)
        
        # --- BẢNG KẾT QUẢ (SỬA LỖI 1: Kéo thả cột) ---
        self.table = QTableWidget()
        self.table.setColumnCount(11)
        self.table.verticalHeader().setVisible(False)
        self.table.setHorizontalHeaderLabels([
            "☑", "STT", "Prompt", "Chế độ", "Model", "Tỷ lệ",
            "Trạng thái", "Preview", "Tiến trình", "Hành động", ""
        ])

        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        header.setSectionResizeMode(9, QHeaderView.ResizeToContents)
        header.setStretchLastSection(True)

        self.table.setColumnWidth(0, 40)
        self.table.setColumnWidth(1, 50)
        self.table.setColumnWidth(2, 260)
        self.table.setColumnWidth(3, 140)
        self.table.setColumnWidth(4, 110)
        self.table.setColumnWidth(5, 80)
        self.table.setColumnWidth(6, 110)
        self.table.setColumnWidth(7, 140)
        self.table.setColumnWidth(8, 150)
        self.table.setColumnWidth(9, 170)
        self.table.setColumnWidth(10, 0)
        self.table.setColumnHidden(10, True)
        
        layout.addWidget(self.table)
        self.setLayout(layout)
        
    def add_task(self, task_id, model, ratio, prompt, count=1):
        row = self.table.rowCount()
        self.table.insertRow(row)
        self.table.setRowHeight(row, 60)
        checkbox = QCheckBox()
        checkbox_widget = QWidget()
        checkbox_layout = QHBoxLayout(checkbox_widget)
        checkbox_layout.addWidget(checkbox)
        checkbox_layout.setAlignment(Qt.AlignCenter)
        checkbox_layout.setContentsMargins(0, 0, 0, 0)
        self.table.setCellWidget(row, 0, checkbox_widget)

        self.table.setItem(row, 1, QTableWidgetItem(str(row + 1)))

        short_prompt = (prompt or "").replace("\n", " ").strip()
        if len(short_prompt) > 80:
            short_prompt = short_prompt[:77] + "..."
        self.table.setItem(row, 2, QTableWidgetItem(short_prompt))

        self.table.setItem(row, 3, QTableWidgetItem(""))

        self.table.setItem(row, 4, QTableWidgetItem(model))
        self.table.setItem(row, 5, QTableWidgetItem(ratio))

        status_item = QTableWidgetItem(f"Chờ (x{count})")
        status_item.setForeground(QColor('orange'))
        self.table.setItem(row, 6, status_item)

        preview_label = QLabel("...")
        preview_label.setAlignment(Qt.AlignCenter)
        self.table.setCellWidget(row, 7, preview_label)

        progress = QProgressBar()
        progress.setValue(0)
        progress.setStyleSheet(
            "QProgressBar { border: 1px solid grey; border-radius: 3px; text-align: center; } "
            "QProgressBar::chunk { background-color: #4CAF50; }"
        )
        self.table.setCellWidget(row, 8, progress)

        action_widget = QWidget()
        action_layout = QHBoxLayout(action_widget)
        action_layout.setContentsMargins(0, 0, 0, 0)
        action_layout.setSpacing(4)
        action_layout.setAlignment(Qt.AlignCenter)

        btn_size = QSize(28, 24)
        icon_size = QSize(18, 18)
        style = QApplication.style()

        run_btn = QPushButton()
        run_btn.setObjectName("runButton")
        run_btn.setFixedSize(btn_size)
        run_btn.setIconSize(icon_size)
        self.update_run_button_style(run_btn)
        run_btn.clicked.connect(self.handle_run_clicked)
        action_layout.addWidget(run_btn)

        regen_btn = QPushButton()
        regen_btn.setToolTip("Tạo lại kết quả")
        regen_btn.setFixedSize(btn_size)
        regen_btn.setIcon(style.standardIcon(QStyle.SP_BrowserReload))
        regen_btn.setIconSize(icon_size)
        regen_btn.setStyleSheet(
            "QPushButton {"
            " background-color: transparent;"
            " border: none;"
            " padding: 0px;"
            "}"
            "QPushButton:hover {"
            " background-color: #e5e7eb;"
            " border-radius: 4px;"
            "}"
        )
        regen_btn.clicked.connect(self.handle_regen_clicked)
        action_layout.addWidget(regen_btn)

        delete_btn = QPushButton()
        delete_btn.setToolTip("Xóa hàng này")
        delete_btn.setFixedSize(btn_size)
        delete_btn.setIcon(style.standardIcon(QStyle.SP_TrashIcon))
        delete_btn.setIconSize(icon_size)
        delete_btn.setStyleSheet(
            "QPushButton {"
            " background-color: transparent;"
            " border: none;"
            " padding: 0px;"
            "}"
            "QPushButton:hover {"
            " background-color: #fee2e2;"
            " border-radius: 4px;"
            "}"
        )
        delete_btn.clicked.connect(self.handle_delete_clicked)
        action_layout.addWidget(delete_btn)

        self.table.setCellWidget(row, 9, action_widget)

        id_item = QTableWidgetItem(task_id)
        self.table.setItem(row, 10, id_item)

        self.tasks[task_id] = {
            "row": row,
            "prompt": prompt,
            "count": count,
            "status": "waiting"
        }

    def set_task_mode(self, task_id, mode_label):
        if task_id not in self.tasks:
            return
        row = self.tasks[task_id]["row"]
        item = self.table.item(row, 3)
        if item is None:
            item = QTableWidgetItem()
            self.table.setItem(row, 3, item)
        item.setText(str(mode_label))
        self.tasks[task_id]["mode_label"] = str(mode_label)

    def update_progress(self, task_id, progress, status, metadata):
        if task_id not in self.tasks:
            return

        task = self.tasks[task_id]
        row = task['row']

        progress_bar = self.table.cellWidget(row, 8)
        if progress_bar and isinstance(progress_bar, QProgressBar):
            if status in ("completed", "error"):
                progress_bar.setValue(progress)
                self.progress_targets.pop(task_id, None)
            else:
                if "start_ts" not in task:
                    task["start_ts"] = time.time()
                task["last_backend_progress"] = progress
                current = progress_bar.value()
                base_target = max(progress, current)
                existing_target = self.progress_targets.get(task_id, 0)
                target = max(base_target, existing_target)
                self.progress_targets[task_id] = target
                if not self.smooth_timer.isActive():
                    self.smooth_timer.start()

        status_item = self.table.item(row, 6)
        if status_item is None:
            status_item = QTableWidgetItem()
            self.table.setItem(row, 6, status_item)

        label_text = ""
        color = QColor("#000000")

        if status == "processing":
            label_text = "Đang xử lý"
            color = QColor("#D35400")
        elif status == "completed":
            label_text = "Hoàn thành"
            color = QColor("#27AE60")
            task.pop("start_ts", None)
            task["last_backend_progress"] = progress
        elif status == "error":
            error_type = (metadata or {}).get("error_type", "")
            if error_type == "prompt_timeout":
                label_text = "Lỗi prompt (timeout)"
            elif error_type == "prompt_error":
                label_text = "Lỗi prompt"
            else:
                label_text = "Lỗi"
            color = QColor("#C0392B")
            task.pop("start_ts", None)
            task["last_backend_progress"] = progress
        else:
            label_text = status or ""
            color = QColor("#000000")

        status_item.setText(label_text)
        status_item.setForeground(color)
        task['status'] = status
    
    def _smooth_progress_tick(self):
        if not self.progress_targets:
            if self.smooth_timer.isActive():
                self.smooth_timer.stop()
            return

        now = time.time()

        for task_id, target in list(self.progress_targets.items()):
            task = self.tasks.get(task_id)
            if not task:
                continue
            if task.get("status") != "processing":
                continue
            start_ts = task.get("start_ts")
            if not start_ts:
                continue
            elapsed = max(0.0, now - start_ts)
            simulated = min(self.auto_progress_max, int(elapsed * 3))
            backend_min = int(task.get("last_backend_progress", 0) or 0)
            current_target = target
            new_target = max(current_target, backend_min, simulated)
            if new_target > current_target:
                self.progress_targets[task_id] = new_target

        finished_keys = []
        for task_id, target in list(self.progress_targets.items()):
            task = self.tasks.get(task_id)
            if not task:
                finished_keys.append(task_id)
                continue

            row = task.get("row")
            if row is None or row < 0 or row >= self.table.rowCount():
                finished_keys.append(task_id)
                continue

            progress_bar = self.table.cellWidget(row, 7)
            if not progress_bar:
                finished_keys.append(task_id)
                continue

            current = progress_bar.value()
            if current >= target:
                if task.get("status") in ("completed", "error"):
                    finished_keys.append(task_id)
                continue

            step = max(1, int((target - current) / 3))
            new_val = current + step
            if new_val > target:
                new_val = target

            if task.get("status") != "completed":
                if new_val >= self.auto_progress_max and target < 100:
                    new_val = min(self.auto_progress_max, target)

            progress_bar.setValue(new_val)

        for k in finished_keys:
            self.progress_targets.pop(k, None)

        if not self.progress_targets and self.smooth_timer.isActive():
            self.smooth_timer.stop()

    def set_preview(self, task_id, data_list, prompt_text="", file_paths=None):
        if task_id not in self.tasks:
            return

        row = self.tasks[task_id]['row']
        preview_label = self.table.cellWidget(row, 7)

        self.tasks[task_id]['full_prompt'] = prompt_text

        if isinstance(file_paths, list) and file_paths:
            self.tasks[task_id]['files'] = list(file_paths)
        elif file_paths is not None:
            self.tasks[task_id]['files'] = []

        if not preview_label:
            return

        source_list = data_list
        if isinstance(file_paths, list) and file_paths:
            source_list = file_paths

        if not isinstance(source_list, list) or len(source_list) == 0:
            preview_label.setText("Không có dữ liệu")
            preview_label.setAlignment(Qt.AlignCenter)
            preview_label.setCursor(Qt.ArrowCursor)
            return

        is_video = task_id.startswith("VID")

        pixmap = QPixmap()
        first_source = source_list[0]
        loaded = False

        if isinstance(first_source, (bytes, bytearray)):
            loaded = pixmap.loadFromData(first_source)
        else:
            try:
                loaded = pixmap.load(first_source)
            except Exception:
                loaded = False

        if (not loaded) or pixmap.isNull():
            if is_video:
                preview_label.setText(f"🎬 {len(source_list)} video")
                preview_label.setAlignment(Qt.AlignCenter)
                preview_label.setCursor(Qt.PointingHandCursor)
                preview_label.mousePressEvent = lambda event, tid=task_id, d=data_list, p=prompt_text: self.show_group_preview(tid, d, p)
            else:
                preview_label.setText("Không xem trước được")
                preview_label.setAlignment(Qt.AlignCenter)
                preview_label.setCursor(Qt.ArrowCursor)
            return

        if len(source_list) > 1:
            base = QPixmap(pixmap)
            p = QPainter(base)

            badge_d = 28
            margin = 4
            x = max(0, base.width() - badge_d - margin)
            y = max(0, base.height() - badge_d - margin)

            p.setBrush(QColor(0, 0, 0, 180))
            p.setPen(Qt.NoPen)
            p.drawEllipse(x, y, badge_d, badge_d)

            p.setPen(QColor("white"))
            font = QFont("Arial", 12, QFont.Bold)
            p.setFont(font)
            p.drawText(x, y, badge_d, badge_d, Qt.AlignCenter, str(len(source_list)))
            p.end()

            pixmap = base

        col_width = self.table.columnWidth(7)
        max_w = max(40, col_width - 10)
        max_h = int(max_w * 1.2)
        scaled_pixmap = pixmap.scaled(
            max_w,
            max_h,
            Qt.KeepAspectRatio,
            Qt.SmoothTransformation
        )

        preview_label.setPixmap(scaled_pixmap)
        preview_label.setAlignment(Qt.AlignCenter)
        preview_label.setCursor(Qt.PointingHandCursor)
        preview_label.mousePressEvent = lambda event, tid=task_id, d=data_list, p=prompt_text: self.show_group_preview(tid, d, p)

    def show_group_preview(self, task_id, data_list, prompt_text):
        if not data_list:
            return

        is_video = task_id.startswith("VID")

        dialog = QDialog(self)
        dialog.setWindowTitle("Preview kết quả")
        dialog.resize(1100, 700)

        main_layout = QHBoxLayout(dialog)

        left_scroll = QScrollArea()
        left_scroll.setWidgetResizable(True)

        img_widget = QWidget()
        grid = QGridLayout(img_widget)
        grid.setContentsMargins(8, 8, 8, 8)
        grid.setSpacing(8)

        labels = []
        max_items = min(len(data_list), 4)

        from PySide6.QtGui import QPainter

        def make_zoom_handler(pix):
            def handler(event):
                if pix.isNull():
                    return
                zoom_dialog = ImageZoomDialog(pix, dialog)
                zoom_dialog.exec()
            return handler

        def make_video_handler(index):
            def handler(event):
                temp_dir = os.path.join(os.getcwd(), "temp_preview_videos")
                os.makedirs(temp_dir, exist_ok=True)
                file_name = f"{task_id}_preview_{index+1}.mp4"
                file_path = os.path.join(temp_dir, file_name)
                try:
                    with open(file_path, "wb") as f:
                        f.write(data_list[index])
                except Exception:
                    QMessageBox.warning(dialog, "Lỗi", "Không lưu được file tạm video để phát.")
                    return
                try:
                    if sys.platform == "win32":
                        os.startfile(file_path)
                    elif sys.platform == "darwin":
                        subprocess.Popen(["open", file_path])
                    else:
                        subprocess.Popen(["xdg-open", file_path])
                except Exception:
                    QMessageBox.warning(dialog, "Lỗi", "Không mở được trình phát video mặc định.")
            return handler

        for i in range(max_items):
            lbl = QLabel()
            lbl.setAlignment(Qt.AlignCenter)
            lbl.setStyleSheet("border: 1px solid #d1d5db; background-color: #111111;")

            if is_video:
                lbl.setText(f"🎬 Video {i+1}")
                lbl.setStyleSheet(
                    "border: 1px solid #d1d5db; "
                    "background-color: #111111; "
                    "color: white; "
                    "padding: 20px;"
                )
                lbl.setCursor(Qt.PointingHandCursor)
                lbl.mousePressEvent = make_video_handler(i)
            else:
                pix = QPixmap()
                pix.loadFromData(data_list[i])
                if not pix.isNull():
                    thumb = pix.scaled(320, 320, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                    lbl.setPixmap(thumb)
                lbl.setCursor(Qt.PointingHandCursor)
                lbl.mousePressEvent = make_zoom_handler(pix)

            labels.append(lbl)

        if max_items == 1:
            grid.addWidget(labels[0], 0, 0, 1, 2)
        elif max_items == 2:
            grid.addWidget(labels[0], 0, 0)
            grid.addWidget(labels[1], 0, 1)
        elif max_items == 3:
            grid.addWidget(labels[0], 0, 0)
            grid.addWidget(labels[1], 0, 1)
            grid.addWidget(labels[2], 1, 0, 1, 2)
        else:
            grid.addWidget(labels[0], 0, 0)
            grid.addWidget(labels[1], 0, 1)
            grid.addWidget(labels[2], 1, 0)
            grid.addWidget(labels[3], 1, 1)

        left_scroll.setWidget(img_widget)
        main_layout.addWidget(left_scroll, 3)

        right_panel = QFrame()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(8, 8, 8, 8)
        right_layout.setSpacing(8)

        prompt_label = QLabel("Prompt:")
        prompt_label.setFont(QFont("Arial", 10, QFont.Bold))
        right_layout.addWidget(prompt_label)

        prompt_edit = QTextEdit()
        if prompt_text:
            prompt_edit.setPlainText(prompt_text)
        else:
            prompt_edit.setPlaceholderText("Nhập hoặc chỉnh sửa prompt dùng để tạo nhóm kết quả này...")
        prompt_edit.setMinimumHeight(200)
        right_layout.addWidget(prompt_edit)

        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        regen_btn = QPushButton("↺ Tạo lại")
        download_btn = QPushButton("Tải về...")
        close_btn = QPushButton("Đóng")
        btn_layout.addWidget(regen_btn)
        btn_layout.addWidget(download_btn)
        btn_layout.addWidget(close_btn)
        right_layout.addLayout(btn_layout)

        main_layout.addWidget(right_panel, 2)

        def handle_download():
            folder = QFileDialog.getExistingDirectory(dialog, "Chọn thư mục lưu")
            if not folder:
                return
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            for idx, data in enumerate(data_list, start=1):
                if is_video:
                    ext = ".mp4"
                else:
                    ext = ".png"
                file_path = os.path.join(folder, f"preview_{ts}_{idx}{ext}")
                try:
                    with open(file_path, "wb") as f:
                        f.write(data)
                except Exception:
                    pass

        def handle_regen():
            new_prompt = prompt_edit.toPlainText().strip()
            if not new_prompt:
                QMessageBox.warning(dialog, "Thiếu Prompt", "Prompt đang trống, vui lòng nhập prompt trước khi tạo lại.")
                return
            self.request_regenerate.emit(task_id, new_prompt)
            dialog.accept()

        regen_btn.clicked.connect(handle_regen)
        download_btn.clicked.connect(handle_download)
        close_btn.clicked.connect(dialog.accept)

        dialog.exec()
    
    def update_run_button_style(self, btn):
        style = QApplication.style()
        if self.mode == "video":
            btn.setToolTip("Chạy hàng này (Tạo video)")
            btn.setIcon(style.standardIcon(QStyle.SP_MediaPlay))
        else:
            btn.setToolTip("Chạy hàng này (Tạo ảnh)")
            btn.setIcon(style.standardIcon(QStyle.SP_FileDialogNewFolder))

        btn.setStyleSheet(
            "QPushButton {"
            " background-color: transparent;"
            " border: none;"
            " padding: 0px;"
            "}"
            "QPushButton:hover {"
            " background-color: #e5e7eb;"
            " border-radius: 4px;"
            "}"
        )

    def set_mode(self, mode):
        if mode not in ("image", "video"):
            return
        self.mode = mode
        for row in range(self.table.rowCount()):
            widget = self.table.cellWidget(row, 8)
            if widget:
                run_btn = widget.findChild(QPushButton, "runButton")
                if run_btn:
                    self.update_run_button_style(run_btn)

    def handle_run_clicked(self):
        row = self.get_row_from_sender()
        if row < 0:
            return
        task_item = self.table.item(row, 10)
        if not task_item:
            return
        task_id = task_item.text()
        if self.mode == "video":
            self.request_run_video.emit(task_id)
        else:
            self.request_run_image.emit(task_id)

    def get_row_from_sender(self):
        btn = self.sender()
        if not isinstance(btn, QPushButton):
            return -1
        parent_widget = btn.parent()
        index = self.table.indexAt(parent_widget.pos())
        return index.row()

    def handle_regen_clicked(self):
        row = self.get_row_from_sender()
        if row < 0:
            return
        self.regenerate_row(row)

    def handle_delete_clicked(self):
        row = self.get_row_from_sender()
        if row < 0:
            return
        self.delete_row(row)

    def regenerate_row(self, row):
        if row < 0 or row >= self.table.rowCount():
            return
        task_item = self.table.item(row, 10)
        if not task_item:
            return
        task_id = task_item.text()
        task_data = self.tasks.get(task_id, {})
        prompt = task_data.get('full_prompt') or task_data.get('prompt', "")
        self.request_regenerate.emit(task_id, prompt)

    def update_task_prompt(self, task_id, new_prompt):
        if task_id in self.tasks:
            self.tasks[task_id]["prompt"] = new_prompt
            self.tasks[task_id]["full_prompt"] = new_prompt
            row = self.tasks[task_id]["row"]
            short_prompt = (new_prompt or "").replace("\n", " ").strip()
            if len(short_prompt) > 120:
                short_prompt = short_prompt[:117] + "..."
            self.table.setItem(row, 2, QTableWidgetItem(short_prompt))

    def mark_task_waiting(self, task_id):
        if task_id not in self.tasks:
            return
        row = self.tasks[task_id]['row']
        status_item = self.table.item(row, 6)
        if status_item is None:
            status_item = QTableWidgetItem()
            self.table.setItem(row, 6, status_item)
        status_item.setText("Chờ (tạo lại)")
        status_item.setForeground(QColor('orange'))
        progress_bar = self.table.cellWidget(row, 7)
        if progress_bar:
            progress_bar.setValue(0)
        self.tasks[task_id]['status'] = 'waiting'

    def show_preview_fullscreen(self, pixmap):
        dialog = QMessageBox(self)
        dialog.setWindowTitle("Preview")
        dialog.setIconPixmap(pixmap.scaled(800, 600, Qt.KeepAspectRatio, Qt.SmoothTransformation))
        dialog.exec()
        
    def toggle_select_all(self, state):
        # 2 là Qt.Checked, 0 là Qt.Unchecked. So sánh trực tiếp với int để chính xác hơn
        is_checked = True if state == 2 else False
        for row in range(self.table.rowCount()):
            widget = self.table.cellWidget(row, 0)
            if widget:
                checkbox = widget.findChild(QCheckBox)
                if checkbox:
                    # Block signal để tránh trigger ngược lại logic không mong muốn nếu có
                    checkbox.blockSignals(True)
                    checkbox.setChecked(is_checked)
                    checkbox.blockSignals(False)
                    
    def select_all(self):
        self.select_all_check.setChecked(True)
                    
    def delete_selected(self):
        rows_to_delete = []
        for row in range(self.table.rowCount()):
            checkbox_widget = self.table.cellWidget(row, 0)
            if checkbox_widget:
                checkbox = checkbox_widget.findChild(QCheckBox)
                if checkbox and checkbox.isChecked():
                    rows_to_delete.append(row)
                    
        for row in sorted(rows_to_delete, reverse=True):
            self.table.removeRow(row)
            
        self.update_task_indices()
        self.select_all_check.setChecked(False)
        
    def delete_row(self, row):
        self.table.removeRow(row)
        self.update_task_indices()
        
    def update_task_indices(self):
        new_tasks = {}
        for row in range(self.table.rowCount()):
            task_id_item = self.table.item(row, 10)
            if task_id_item:
                task_id = task_id_item.text()
                if task_id in self.tasks:
                    task_data = self.tasks[task_id]
                    task_data['row'] = row
                    new_tasks[task_id] = task_data
                self.table.item(row, 1).setText(str(row + 1))
        self.tasks = new_tasks
        
    def clear_all(self):
        self.table.setRowCount(0)
        self.tasks.clear()
        self.select_all_check.setChecked(False)
        
    def get_column_widths(self):
        widths = []
        for i in range(self.table.columnCount()):
            widths.append(self.table.columnWidth(i))
        return widths
        
    def set_column_widths(self, widths):
        if not widths: return
        for i, width in enumerate(widths):
            if i < self.table.columnCount():
                self.table.setColumnWidth(i, width)

class KeyVerificationDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Xác thực bản quyền - Auto VEO3")
        
        # SỬA LỖI: Thay vì setFixedSize(400, 180), ta chỉ set chiều rộng tối thiểu
        # Chiều cao sẽ tự động giãn ra cho đủ nội dung
        self.setMinimumWidth(450)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        
        layout = QVBoxLayout()
        layout.setSpacing(15)
        layout.setContentsMargins(25, 25, 25, 25)

        # Title
        title = QLabel("🔐 Nhập License Key")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #1e293b;")
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)

        # Input
        self.key_input = QLineEdit()
        self.key_input.setPlaceholderText("Dán key của bạn vào đây...")
        # SỬA LỖI: Thêm min-height để ô nhập không bị bẹp dúm
        self.key_input.setStyleSheet("""
            QLineEdit {
                padding: 10px 15px;
                border: 2px solid #e2e8f0;
                border-radius: 8px;
                font-size: 14px;
                min-height: 25px; /* Đảm bảo chiều cao tối thiểu */
            }
            QLineEdit:focus {
                border-color: #6366f1;
            }
        """)
        layout.addWidget(self.key_input)

        # Button
        self.check_btn = QPushButton("Kiểm tra & Đăng nhập")
        self.check_btn.setCursor(Qt.PointingHandCursor)
        # SỬA LỖI: Thêm min-height cho nút bấm đẹp hơn
        self.check_btn.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #3b82f6, stop:1 #2563eb);
                color: white;
                padding: 12px;
                border-radius: 8px;
                font-weight: bold;
                font-size: 14px;
                border: none;
                min-height: 25px;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #2563eb, stop:1 #1d4ed8);
            }
            QPushButton:disabled {
                background-color: #94a3b8;
            }
        """)
        self.check_btn.clicked.connect(self.verify_key)
        layout.addWidget(self.check_btn)

        self.status_label = QLabel("")
        self.status_label.setAlignment(Qt.AlignCenter)
        # Set font size cho status để dễ đọc
        self.status_label.setStyleSheet("font-size: 13px; font-weight: 500;")
        layout.addWidget(self.status_label)

        # Thêm khoảng trống co giãn ở dưới cùng để đẩy nội dung lên trên đẹp mắt
        layout.addStretch()

        self.setLayout(layout)
        
        # Tự động điều chỉnh kích thước cửa sổ cho vừa khít nội dung
        self.adjustSize()

    def verify_key(self):
        user_key = self.key_input.text().strip()
        if not user_key:
            self.status_label.setText("⚠️ Vui lòng nhập key!")
            self.status_label.setStyleSheet("color: #d97706; font-size: 13px; font-weight: 500;")
            return

        self.check_btn.setEnabled(False)
        self.check_btn.setText("Đang kết nối server...")
        self.status_label.setText("Vui lòng đợi giây lát...")
        self.status_label.setStyleSheet("color: #475569; font-size: 13px;")
        QApplication.processEvents()

        url = "https://gist.githubusercontent.com/visecal/0bbdfc4abf1007f2f73fb6e13060bb66/raw/7ab11ddf5b2e5d645ffc74a93a0bf5068ddb3526/gistfile1.txt"

        try:
            # Thêm random param để tránh cache
            response = requests.get(f"{url}?t={int(time.time())}", timeout=15)
            
            if response.status_code == 200:
                valid_keys = [line.strip() for line in response.text.splitlines() if line.strip()]
                
                if user_key in valid_keys:
                    self.status_label.setText("✅ Key chính xác! Đang vào...")
                    self.status_label.setStyleSheet("color: #10b981; font-weight: bold;")
                    QApplication.processEvents()
                    time.sleep(0.8)
                    self.accept()
                else:
                    self.status_label.setText("❌ Key không hợp lệ!")
                    self.status_label.setStyleSheet("color: #ef4444; font-weight: bold;")
            else:
                self.status_label.setText(f"❌ Lỗi Server: {response.status_code}")
                self.status_label.setStyleSheet("color: #ef4444;")

        except Exception as e:
            self.status_label.setText("❌ Không có kết nối mạng!")
            self.status_label.setStyleSheet("color: #ef4444;")
            print(e)
        
        finally:
            self.check_btn.setEnabled(True)
            if self.result() != QDialog.Accepted:
                self.check_btn.setText("Kiểm tra & Đăng nhập")

def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')

    # --- BẮT ĐẦU ĐOẠN CODE KIỂM TRA KEY ---
    login_dialog = KeyVerificationDialog()
    if login_dialog.exec() == QDialog.Accepted:
        # Nếu nhập đúng key thì mới khởi tạo cửa sổ chính
        window = AccountManager()
        window.show()
        return app.exec()
    else:
        # Nếu tắt bảng nhập key hoặc nhập sai mà thoát
        return 0
    # --- KẾT THÚC ĐOẠN CODE KIỂM TRA KEY ---

if __name__ == '__main__':
    main()
