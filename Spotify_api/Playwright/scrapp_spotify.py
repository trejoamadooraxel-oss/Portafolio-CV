import Spotify_api.Conexiones.conection_playwright as con_playwright
import re

def limpiar_numero(texto):
    """Extrae solo dígitos de un string, sea '27.745' o '1.176 oyentes'."""
    if not texto:
        return None
    solo_digitos = re.sub(r'[^\d]', '', texto)
    return int(solo_digitos) if solo_digitos else None


def verifi_conection_playwrigth():

    try:
        # va en una tarea de verificacion de verifiacion en Airflow
        conection_p = con_playwright.Conection_playwright()
        print('Exito en conexion con Playwrithg.')

    except Exception as e:
        print('No se pudo establecer una conexion con Playwrithg', e)
        return None

    return conection_p

def extract_artist_inf(conection_p, url, list_artist, Sync_Artist):

    #va en una tarea de verificacion de verifiacion en Airflow
    time_await_defaut = 1000
    info_artist = []

    try:
        page = conection_p.get_page()
        page.goto(url)

        for artist in list_artist:
            page.locator('[data-testid="home-button"]').click()

            page.locator('[data-testid="home-button"]').click()

            print(f'Ingresamos en el buscador a: {artist}.')
            # 1. Localizamos la caja de búsqueda de forma más segura
            buscador = page.locator('[role="combobox"], input[data-testid="search-input"]').first
            buscador.click()
            page.wait_for_timeout(time_await_defaut)

            # Limpiamos e ingresamos el texto simulando pulsaciones reales
            buscador.fill('')
            buscador.type(f'{artist}', delay=100)  # El delay despierta los eventos de la página
            page.wait_for_timeout(time_await_defaut * 2)

            # Presionamos Enter por si la interfaz móvil/headless requiere confirmación
            buscador.press("Enter")
            page.wait_for_timeout(time_await_defaut * 2)

            print('Seleccionamos apartado de Artistas.')
            # 2. Selector alternativo ultra-robusto por si 'span' no responde en headless
            pestana_artistas = page.locator('span, a').filter(has_text="Artistas").first
            page.wait_for_timeout(time_await_defaut * 3)
            if pestana_artistas.count() == 0:
                # Respaldo en inglés por si el locale no se inyectó a tiempo en la sesión
                pestana_artistas = page.locator('span, a').filter(has_text="Artists").first

            page.wait_for_timeout(time_await_defaut * 3)
            # Esperamos que sea rastreable antes de forzar el clic
            pestana_artistas.wait_for(state="attached", timeout=10000)
            pestana_artistas.click(force=True)
            page.wait_for_timeout(time_await_defaut * 3)

            print('Validando que exista el artista a buscar.')
            page.wait_for_selector('[data-testid="search-category-card-0"]', timeout=15000)

            # Debug: qué títulos hay realmente en pantalla
            titulos = page.locator('p[data-encore-id="cardTitle"]').all_inner_texts()
            print(f'Tarjetas encontradas: {titulos}')

            tarjeta_artista = page.locator(f'p[title="{artist}"]').first
            tarjeta_artista.wait_for(state="visible", timeout=10000)
            tarjeta_artista.click(force=True)

            print('Nos movemos al elemento de Acerc de:')
            page.wait_for_timeout(time_await_defaut * 10)
            acerca_de = page.locator('[data-encore-id="text"]').filter(has_text="Información")
            acerca_de.scroll_into_view_if_needed()

            nodo_padre = acerca_de.locator('../div')
            nodo_padre.click()
            page.wait_for_timeout(time_await_defaut * 3)

            nodo_seguidores = page.locator('[data-encore-id="text"]').filter(has_text="Seguidores")
            seguidores = nodo_seguidores.locator("..").locator("div").first.inner_text()

            nodo_oyentes = page.locator('//div[@class="e-10860-text encore-text-body-small"]').filter(has_text="oyentes")
            ciudades = []
            oyentes_ciudad = []
            for i in range(nodo_oyentes.count()):
                if i == 0:
                    escuchas_mensuales = nodo_oyentes.nth(i).locator("..").locator("div").first.inner_text()
                else:
                    ciudades.append(nodo_oyentes.nth(i).locator("..").locator("div").first.inner_text())
                    oyentes_ciudad.append(nodo_oyentes.nth(i).first.inner_text())

            info_artist.append({

                'id_artista': Sync_Artist.id_db(artist),
                'artista': artist,
                'seguidores': limpiar_numero(seguidores),
                'escuchas_mensuales': limpiar_numero(escuchas_mensuales),
                'ciudad_oyente_uno': ciudades[0].strip(),
                'num_oyentes_uno': limpiar_numero(oyentes_ciudad[0]),
                'ciudad_oyente_dos': ciudades[1].strip(),
                'num_oyentes_dos': limpiar_numero(oyentes_ciudad[1]),
                'ciudad_oyente_tres': ciudades[2].strip(),
                'num_oyentes_tres': limpiar_numero(oyentes_ciudad[2]),
                'ciudad_oyente_cuatro': ciudades[3].strip(),
                'num_oyentes_cuatro': limpiar_numero(oyentes_ciudad[3]),
                'ciudad_oyente_cinco': ciudades[4].strip(),
                'num_oyentes_cinco': limpiar_numero(oyentes_ciudad[4]),
            })

            page.keyboard.press("Escape")
            page.wait_for_timeout(time_await_defaut * 2)


        return info_artist

    except TimeoutError:
            print('Timeout durante la buscar del artista.')

    except Exception as e:
        print('Error inesperado durante la bsuqueda del artista:', e)


def main():
    url = "https://open.spotify.com/"
    list_artist = ['EXO', 'Bad Bunny', 'Hocico']

    conection_p = verifi_conection_playwrigth()
    list_info = extract_artist_inf(conection_p, url, list_artist)
    print(list_info)

if __name__ == "__main__":
    main()
