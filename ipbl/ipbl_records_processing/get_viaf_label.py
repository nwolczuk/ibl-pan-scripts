from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class VIAFClient:
    def __init__(self, headless: bool = True):
        self.chrome_options = Options()

        if headless:
            self.chrome_options.add_argument("--headless=new")

        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument("--no-sandbox")

        self.chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )

        self.driver = webdriver.Chrome(options=self.chrome_options)

    def get_label(self, viaf_url: str, timeout: int = 15) -> str | None:
        try:
            self.driver.get(viaf_url)

            div = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.heading"))
            )

            return div.text.strip()

        except Exception as e:
            # print(f"Błąd VIAF: {e}")
            return None

    def close(self):
        self.driver.quit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()