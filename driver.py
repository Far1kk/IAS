from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
import pickle


software = [SoftwareName.CHROME.value]
operating_sys = [OperatingSystem.WINDOWS.value]
user_agent_rotator = UserAgent(software_names=software, operating_system=operating_sys, limit=100)

class GetDriver:
    """
    Подключение драйвера
    Основное применение - подключение драйвера и загрузка cookie
    """

    def __init__(self, path_driver: str, path_cookie=None):
        """
        :param path_driver: путь до exe-файла драйвера
        :param path_cookie: путь до необходимого coocke-файла
        """
        self.path_driver = path_driver
        self.path_cookie = path_cookie
    def driver(self):
        """
        Метод для инициализации драйвера, настройки драйвера
        :return: Возвращает драйвер для последующего подключения
        """
        try:
            service = ChromeService(executable_path=self.path_driver)
            options = webdriver.ChromeOptions()
            options.add_argument("start-maximized")
            options.add_argument(f"user-agent={user_agent_rotator.get_random_user_agent()}")
            driver = webdriver.Chrome(options=options, service=service)
            if not self.path_cookie.endswith('.pkl'):
                options.add_argument(f"user-data-dir={self.path_cookie}")
                driver = webdriver.Chrome(options=options, service=service)
            elif self.path_cookie.endswith('.pkl'):
                driver.get(url=u"https://www.wildberries.ru/catalog/muzhchinam")
                cookies = pickle.load(open(self.path_cookie, "rb"))
                for cookie in cookies:
                    driver.add_cookie(cookie)
            else:
                print('Куки не найдены')
            print('Подключение драйвера успешно выполнено')
            return driver
        except Exception as ex:
            print(ex)
            driver.close()
            driver.quit()