import conection_playwright as cp
import scraping_spotify


def main():
    url = "https://open.spotify.com/"
    time_await_defaut = 1000
    artists = ['Geometric Vision', 'Mareux', 'José José']



    conection_p = cp.Conection_playwright()
    page = conection_p.get_page()
    page.goto(url)
    song_caracters = scraping_spotify.top_ten_song_artists(page,artists,time_await_defaut)
    print(song_caracters)
    song_albunes = scraping_spotify.all_albuns_artists(page,artists,time_await_defaut)
    print(song_albunes)

    conection_p.close_browser()
    conection_p.close_conection_p()

if __name__ == "__main__":
    main()

