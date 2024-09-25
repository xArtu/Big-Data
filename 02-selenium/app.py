from selenium import webdriver
import time

# Inicializa el navegador Chrome
driver = webdriver.Chrome()
time.sleep(150)

# Abre una página web
driver.get('https://es.wikipedia.org/wiki/Wikipedia:Portada')
time.sleep(150)

# Interactúa con un elemento de la página
campo_texto = driver.find_element_by_class_name('cdx-text-input__input')
campo_texto.send_keys('Escuela Superior de Cómputo')

time.sleep(150)
menu_items = driver.find_elements_by_css_selector('cdx-menu_listbox')
menu_items[0].click()

time.sleep(150)
driver.quit()