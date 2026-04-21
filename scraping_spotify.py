from playwright.sync_api import sync_playwright, Error, TimeoutError

def search_artist(page, artist, time_await_defaut):
    
    try:
        page.locator('[data-testid="home-button"]').click()

        print(f'Ingresamos en el buscador a: {artist}.')
        page.locator('[role="combobox"]').click()
        page.wait_for_timeout(time_await_defaut * 3)
        page.locator('[role="combobox"]').fill(f'{artist}')
        page.wait_for_timeout(time_await_defaut * 10)

        print('Valdiando que exista el artista a buscar.')
        card_results = page.locator('[data-testid="top-result-card"]')
        a_tag = card_results.locator('a').first
        artist_tag = a_tag.locator('div')
        name_artist = artist_tag.first.inner_text()
        artist_tag.click()

    except TimeoutError:
            print('Timeout durante la buscar del artista.')

    except Error as e:
        print('Error Playwright durante la busqueda del artista:', e)

    except Exception as e:
        print('Error inesperado durante la bsuqueda del artista:', e)

def top_ten_song_artists(conection_p, url, artists, time_await_defaut):

    page = conection_p.get_page()
    page.goto(url)
    song_caracters = []

    for artist in artists:

        search_artist(page, artist, time_await_defaut)

        print(f'Buscando a: {artist}')

        #Obtenemos la lista de las canciones mas reproducidas del artista y sus atributos.
        print('Obtenemos las lista de canciones mas escuchadas.')
        try:
            page.get_by_text('Ver más').click()
            page.wait_for_timeout(time_await_defaut * 3)
            card_list_songs = page.locator('[role="presentation"] div[data-testid="tracklist-row"]')
            print(card_list_songs.count())

            for i in range(card_list_songs.count()):

                atributos = card_list_songs.nth(i).locator('[role=gridcell] div[data-encore-id="text"]')
                
                if atributos.count() == 3:
                    #for atributo in range(atributos.count()):
                    song_name = atributos.nth(0).inner_text()
                    song_num_repoduccion = atributos.nth(1).inner_text()
                    song_time = atributos.nth(2).inner_text()
                
                    song_caracters.append({
                        'artist': artist,
                        'name song': song_name.replace(',','').strip(),
                        'num reproduction': song_num_repoduccion.replace(',','').strip(),
                        'time duration': song_time.replace(',','').strip()
                    })

                else:
                    raise('El numero de atributos cambio ya no es 3, revisar el portal')

        except TimeoutError:
            print(f'Timeout no pudo cargar un elmento durante la extracion del top 10 del artista: {artist}.')

        except Error as e:
            print(f'Error Playwright durante extracion del top 10 del artista: {artist}:', e)

        except Exception as e:
            print('Error inesperado:', e)
        
    return song_caracters 
        
def all_albuns_artists(conection_p, url, artists, time_await_defaut):

    page = conection_p.get_page()
    page.goto(url)
    song_albunes = []

    for artist in artists:

        search_artist(page, artist, time_await_defaut)
    
        #Obtenemos la discografia del artista y sus atributos.
        print('Obtenemos toda la discografia.')
        try:
            
            page_discografia = page.locator('[aria-label="Discografía"] span[data-encore-id="text"]')
            page_discografia.click()
            page.wait_for_timeout(time_await_defaut * 3)
            page.get_by_text("Fecha de lanzamiento").click()
            page.wait_for_timeout(time_await_defaut * 3)
            page.get_by_text("Cuadrícula").click()
            page.wait_for_timeout(time_await_defaut * 5)

            all_discografia = page.locator('[role="presentation"] div[data-encore-id="card"]')
            for j in range(all_discografia.count()):
                disc_name = all_discografia.nth(j).locator('[data-encore-id="cardTitle"]').get_attribute('title')
                disc_type = all_discografia.nth(j).locator('[data-encore-id="cardSubtitle"]').text_content()

                song_albunes.append({
                        'artist': artist,
                        'name album': disc_name.replace(',','').strip(),
                        'year': disc_type.split(' ')[0].replace(',','').strip(),
                        'type_disc': disc_type.split(' ')[-1].replace(',','').strip()
                    })
            
        except TimeoutError:
            print('Timeout: no apareció el elemento')

        except Error as e:
            print('Error Playwright:', e)

        except Exception as e:
            print('Error inesperado:', e)   

    return song_albunes



