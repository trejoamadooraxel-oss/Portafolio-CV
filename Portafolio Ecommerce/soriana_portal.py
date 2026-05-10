import conection_playwright as playwright

time_await_defaut = 1000

def get_url(url):

    conn_play = playwright.Conection_playwright()
    page = conn_play.get_page()
    page.goto(url)
    return page

def main():
    page = get_url('https://www.soriana.com/')
    page.wait_for_timeout(time_await_defaut * 10)


if __name__ == "__main__":
    main()
