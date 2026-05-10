from playwright.sync_api import sync_playwright, Error, TimeoutError

class Conection_playwright():
    def __init__(self):
        self.conection_p = sync_playwright().start()
        self.browser = self.conection_p.chromium.launch(headless=False)
        self.page = self.browser.new_page()

    def get_page(self):
        return self.page

    def close_browser(self):
        self.browser.close()

    def close_conection_p(self):
        self.conection_p.stop() 