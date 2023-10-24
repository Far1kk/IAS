from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
import pickle
URLAUTH = "https://passport.yandex.ru/auth/list"
URLAUTH = "https://m.vk.com/feed"

def savecookie(url):
    try:
        service = ChromeService(executable_path=r"C:\Users\bogdan\parser_wordstat\driver\chromedriver.exe")
        options = webdriver.ChromeOptions()
        options.add_argument("start-maximized")
        driver = webdriver.Chrome(options=options, service=service)
        driver.get(url)
        #driver.find_element("QR-код").click()
        flag = input()
        options.add_argument(r"user-data-dir=C:\Users\bogdan\parser_wordstat\cock")
        driver.get(url)
        # while flag == False:
        #     pass
        #pickle.dump(driver.get_cookies(), open("cookies.pkl", "wb"))
    except Exception as ex:
        print(ex)
    finally:
        driver.close()
        driver.quit()

if __name__ == '__main__':
    savecookie(URLAUTH)