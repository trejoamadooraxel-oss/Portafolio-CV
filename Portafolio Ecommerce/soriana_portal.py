from undetected_chromedriver.devtool import timeout
from pathlib import Path
import conection_playwright as playwright
import pandas as pd
from datetime import datetime
from datetime import datetime


time_await_defaut = 1000



def search_products(page, url):

    print(f'Obteniendo sub categorias')

    # Lista de categorias que se muestran una vez cargado el elemento, solo sirve para poder obtener ls sub-categorias de ello
    subcategories = page.locator('div[class="c-category-carousel__title"] h3')

    txt_subcategories = subcategories.all_inner_texts()

    categoria = page.locator('h1[class="margin-generic"]').inner_text()
    lista_productos = []
    diccionario_productos = {}
    #iteramos sobre las categorias
    for i in range(subcategories.count()):
        subcategoria = subcategories.nth(i).inner_text()
        print(f'Ingresando a categoria de: {subcategoria}')

        # Regresamos al nodo padre con '..' del xpath subcategoria y realizamos la busqueda del elemento con ver mas que es el boton
        subcategories.nth(i).locator('..').get_by_text('Ver más').click()
        page.wait_for_timeout(time_await_defaut * 5)

        while True:
            all_products = page.locator(
                'div[class="col-12 col-sm-3 col-md-3 product-tile--wrapper d-flex list-item-product pb-1"]')
            for index_producto in range(all_products.count()):
                #print(f'Numero de producto: {index_producto}')
                producto = all_products.nth(index_producto)
                producto.scroll_into_view_if_needed()
                url_product = all_products.nth(index_producto).locator('a').nth(1).get_attribute('href')
                name_product = all_products.nth(index_producto).locator('a').nth(1).inner_text()
                try:
                    page.wait_for_timeout(time_await_defaut * 1)
                    sku_product = all_products.nth(index_producto).locator(
                        'img[data-src]').get_attribute('src')
                except TimeoutError:
                    raise Exception('Error. Al obtener el SKU de la imagen.')
                try:
                    try:
                        price_product_origin = all_products.nth(index_producto).locator(
                            'div[class="list d-flex align-items-center inline-max-content-disabled align-self-start size-price-content align-self-lg-center"] span span').inner_text(
                            timeout=time_await_defaut)
                    except:
                        price_product_origin = all_products.nth(index_producto).locator(
                            'div[class="sales d-flex align-items-center flex-row-reverse align-self-start align-self-lg-end size-price-content  badget_d   null "] span span').inner_text(
                            timeout=time_await_defaut)

                    try:

                        price_producto_ofert = all_products.nth(index_producto).locator(
                            'div[class="sales d-flex align-items-center flex-row-reverse align-self-start align-self-lg-end size-price-content special-price-badge d-flex  discountPrice oldDiscountPrice null "] span span').inner_text(
                            timeout=time_await_defaut)
                    except:
                        price_producto_ofert = all_products.nth(index_producto).locator(
                            'div[class="sales d-flex flex-row-reverse float-right discountPrice mt-0 align-items-center"] span').inner_text(
                            timeout=time_await_defaut)

                except Exception as e:

                    try:
                        price_product_origin = all_products.nth(index_producto).locator(
                            'div [class="sales d-flex align-items-center flex-row-reverse align-self-start align-self-lg-end size-price-content  d-flex   null "] span span').inner_text(
                            timeout=time_await_defaut)
                        price_producto_ofert = 'vacio'
                    except TimeoutError:

                        raise Exception('Error. El xpath para el precio y precio ofertacambio.')

                #Dentro de las descripcion del producto estan cuatro apartados como tipo promociones, se obtuvieron cada
                #uno de ellos y cada uno viene en un carrusel diferente.
                carrusel_promotion = all_products.nth(index_producto).locator('div[class="carousel-inner"]')
                #if carrusel_promotion.count() == 4:
                try:
                    promo_product = carrusel_promotion.nth(1).locator('div[class="product-badge"] span').nth(0).inner_text(timeout=time_await_defaut)
                    #print(promo_product)
                except Exception as e:
                    promo_product = 'vacio'
                try:
                    type_promo_by_product = carrusel_promotion.nth(1).locator(
                        'div[class="product-badge badge-div-coupons"] span').nth(0).inner_text(timeout=time_await_defaut)
                    #print(type_promo_by_product)
                except Exception as e:
                    type_promo_by_product = 'vacio'
                try:
                    bonus_product = carrusel_promotion.nth(2).locator('div[class="product-badge"] span').nth(0).inner_text(timeout=time_await_defaut)
                    #print(bonus_product)
                except Exception as e:
                    bonus_product = 'vacio'
                try:
                    type_bonus_by_product = carrusel_promotion.nth(3).locator('div[class="product-badge"] span').nth(
                        0).inner_text(timeout=time_await_defaut)
                    #print(type_bonus_by_product)
                except Exception as e:
                    type_bonus_by_product = 'vacio'



                diccionario_productos={
                    'categoria': categoria,
                    'sub_categoria': subcategoria,
                    'nombre_product': name_product,
                    'sku': sku_product,
                    'url_general':url,
                    'url_product': url_product,
                    'price_original' : price_product_origin,
                    'precio_oferta': price_producto_ofert,
                    'promo_producto' : promo_product,
                    'type_promo_by_product' : type_promo_by_product,
                    'bonus_product': bonus_product,
                    'type_bonus_by_product' : type_bonus_by_product,
                    'time_update': datetime.now().strftime("%Y-%m-%d")
                }

                lista_productos.append(diccionario_productos)

            try:
                print('Haciendo Click a la siguiente pagina.')
                btn_next_page = page.locator('button[class="slick-next pagination show-more new-plp-design slick-arrows"]')
                btn_next_page.scroll_into_view_if_needed()
                btn_next_page.click()
                page.wait_for_timeout(time_await_defaut * 5)

            except Exception as e:
                print("No hay más páginas o error:", e)
                break

        #break
        page.goto(url)
        page.wait_for_timeout(time_await_defaut * 5)



    df = pd.DataFrame(lista_productos)
    df.to_csv(f'{Path(__file__).parent}/files_portafolio/soriana_{categoria}.csv', encoding='utf8', index=False)


"""def main():


    url = 'https://www.soriana.com/electronica/tv-y-video/'
    page = get_url(url)
    search_products(page, url)


if __name__ == "__main__":
    main()
"""