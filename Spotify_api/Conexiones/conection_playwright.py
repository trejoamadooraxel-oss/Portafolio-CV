from playwright.sync_api import sync_playwright, Error, TimeoutError



class Conection_playwright():
    def __init__(self,headless=True):
        self.conection_p = sync_playwright().start()
        self.browser = self.conection_p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"]
        )
        self.context = self.browser.new_context(
            locale="es-ES",
            timezone_id="America/Mexico_City",
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        self.page = self.context.new_page()

    def get_page(self):
        return self.page

    def close_browser(self):
        try:
            self.context.close()
            self.browser.close()
        except Exception as e:
            print(f"Error al cerrar el navegador: {e}")

    def close_conection_p(self):
        try:
            self.conection_p.stop()
        except Exception as e:
            print(f"Error al detener Playwright: {e}")