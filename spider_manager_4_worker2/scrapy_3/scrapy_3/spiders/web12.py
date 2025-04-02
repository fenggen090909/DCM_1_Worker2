import scrapy
import scrapy
import logging
from scrapy import signals
import logging
from scrapy_3.items import Scrapy3ItemStackOverflow
from scrapy_3.pipe.pipelines12 import Scrapy3Pipeline
import redis
from scrapy.utils.project import get_project_settings
import json 
import signal
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from scrapy_3.db.models import Parameters
import sys


class Web12Spider(scrapy.Spider):
    name = "web12spider"    
    allowed_domains = ["stackoverflow.com"]

    custom_settings = {
        'CONCURRENT_REQUESTS': 16,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
        'ITEM_PIPELINES': {            
            "scrapy_3.pipe.pipelines12.Scrapy3Pipeline": 300,            
        }
    }

    task_id_c = None
    spider_c = None
    ip_c = None
    docker_id_c = None 
    worker_id_c = None

    def __init__(self, **kwargs):
        logging.critical("fg web12 __init__ 0")
        self.logger.critical("fg web12 __init__ 0 - using self.logger")
        print("fg web12 __init__ 0 - using print")

        # 保存传入的参数
        self.detail_url = None
        self.question_id = None
        self.title = None
        self.task_id_c = None
        self.spider_c = None
        self.ip_c = None
        self.docker_id_c = None 
        self.worker_id_c = None

        for key, value in kwargs.items():
            logging.critical(f"key={key} value={value}")
            setattr(self, key, value)  # 直接设置属性

        # settings = get_project_settings()
        params = self.load_params_from_db()
        for key, value in params.items():
            setattr(self, key, value) 
        # logging.critical(f"fenggen --page={self.page}")  

        try:
            self.redis_conn = redis.Redis(
                host = self.REDIS_HOST,
                port = self.REDIS_PORT,
            )
        except Exception as e:
            logging.critical(f"fenggen redis conn error {e}")
        
        logging.critical("redis conn sucessful")                

        pass

    def load_params_from_db(self):
            # 数据库读取逻辑，类似前面的例子
                
            try:                
                engine_questionshunt = create_engine('postgresql://postgres:Fg285426*@192.168.0.58:5432/questionshunt')            
                with engine_questionshunt.connect() as conn:
                    pass

            except Exception as e:
                logging.critical(f"fenggen engine_questionshunt.connect error {e}")        
                
            session_questionshunt = sessionmaker(bind=engine_questionshunt)()

            params_query = session_questionshunt.query(Parameters).filter_by(crawler_name=self.name).all()

            params_dict = {}

            for param in params_query:
                # 根据参数类型进行转换
                if param.parameter_type == 'integer':
                    value = int(param.parameter_value)
                elif param.parameter_type == 'float':
                    value = float(param.parameter_value)
                elif param.parameter_type == 'boolean':
                    value = param.parameter_value.lower() == 'true'
                elif param.parameter_type == 'json':
                    value = json.loads(param.parameter_value)
                else:
                    value = param.parameter_value

                params_dict[param.parameter_key] = value
                logging.critical(f"key={param.parameter_key} val={value}")

            logging.critical(f"reading sucessful [web11spider]  {len(params_dict)} ")
            return params_dict 

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        logging.critical("fg web12 from_crawler 5")
        spider = super(Web12Spider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.engine_started, signal=signals.engine_started)
        crawler.signals.connect(spider.engine_stopped, signal=signals.engine_stopped)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(spider.spider_error, signal=signals.spider_error)
        crawler.signals.connect(spider.request_scheduled, signal=signals.request_scheduled)
        crawler.signals.connect(spider.request_dropped, signal=signals.request_dropped)
        crawler.signals.connect(spider.request_reached_downloader, signal=signals.request_reached_downloader)
        crawler.signals.connect(spider.response_received, signal=signals.response_received)
        crawler.signals.connect(spider.response_downloaded, signal=signals.response_downloaded)
        crawler.signals.connect(spider.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(spider.item_dropped, signal=signals.item_dropped)
        crawler.signals.connect(spider.item_error, signal=signals.item_error)
        crawler.signals.connect(spider.stats_spider_opened, signal=signals.stats_spider_opened)
        crawler.signals.connect(spider.stats_spider_closing, signal=signals.stats_spider_closing)
        crawler.signals.connect(spider.stats_spider_closed, signal=signals.stats_spider_closed)
        crawler.signals.connect(spider.headers_received, signal=signals.headers_received)
        crawler.signals.connect(spider.bytes_received, signal=signals.bytes_received)

        # crawler.signals.connect(spider.signal_handler, signal=signal.SIGINT)
        
        # crawler.signals.connect(spider.offsite_request_dropped, signal=signals.offsite_request_dropped)
        # crawler.signals.connect(spider.update_telnet_vars, signal=signals.update_telnet_vars)
        # crawler.signals.connect(spider.capture_file_opened, signal=signals.capture_file_opened)
        # crawler.signals.connect(spider.capture_file_closed, signal=signals.capture_file_closed)

        return spider

    def engine_started(self):
        logging.critical("fg web12 engine_started 10")
        pass

    def engine_stopped(self):
        logging.critical("fg web12 engine_stopped 20")
        pass

    def spider_opened(self, spider):
        logging.critical("fg web12 spider_opened 30")
        pass

    def spider_idle(self, spider):
        logging.critical("fg web12 spider_idle 40")
        pass

    def spider_closed(self, spider):
        logging.critical("fg web12 spider_closed 50")
        pass
    

    def spider_error(self, spider):
        logging.critical("fg web12 spider_error 60")
        pass

    def request_scheduled(self, spider):
        logging.critical("fg web12 request_scheduled 70")
        pass

    def request_dropped(self, spider):
        logging.critical("fg web12 request_dropped 80")
        pass

    def request_reached_downloader(self, spider):
        logging.critical("fg web12 request_reached_downloader 90")
        pass

    def response_received(self, spider):
        logging.critical("fg web12 response_received 100")
        pass
    

    def response_downloaded(self, spider):
        logging.critical("fg web12 response_downloaded 110")
        pass

    def item_scraped(self, item, response, spider):
        logging.critical("fg web12 item_scraped 120")        
        pass

    def item_dropped(self, spider):
        logging.critical("fg web12 item_dropped 130")
        pass

    def item_error(self, spider):
        logging.critical("fg web12 item_error 140")
        pass

    def stats_spider_opened(self, spider):
        logging.critical("fg web12 stats_spider_opened 150")
        pass


    def stats_spider_closing(self, spider):
        logging.critical("fg web12 stats_spider_closing 160")
        pass

    def stats_spider_closed(self, spider):
        logging.critical("fg web12 stats_spider_closed 170")
        pass

    def headers_received(self, spider):
        logging.critical("fg web12 headers_received 180")
        pass

    def bytes_received(self, spider):
        logging.critical("fg web12 bytes_received 190")
        pass

    def offsite_request_dropped(self, spider):
        logging.critical("fg web12 offsite_request_dropped 200")
        pass


    def update_telnet_vars(self, spider):
        logging.critical("fg web12 update_telnet_vars 210")
        pass

    def capture_file_opened(self, spider): 
        logging.critical("fg web12 capture_file_opened 220")
        pass

    def capture_file_closed(self, spider):
        logging.critical("fg web12 capture_file_closed 230")
        pass   

    # def signal_handler(sig, frame):
    #     print('You pressed Ctrl+C!')
    #     # 在这里添加清理代码，例如关闭数据库连接、保存数据等
    #     self.redis_client.close()
    #     sys.exit(0)

    # signal.signal(signal.SIGINT, signal_handler)                   

    def start_requests(self):
        logging.critical("fg web12 start_requests 240")  
        # sys.stdout.flush()
        # sys.stderr.flush()
        
        # 使用初始化时接收的参数直接开始爬取
        if self.detail_url and self.question_id:
            logging.critical(f"Using provided parameters: detail_url={self.detail_url}, question_id={self.question_id}")
            yield scrapy.Request(
                url=self.detail_url,
                callback=self.parse,
                errback=self.error_handler,
                dont_filter=True
            )
        else:
            error_message = "detail_url or question_id is missing. Both must be provided."
            logging.error(error_message) # Log the error
            raise ValueError(error_message) # Raise a ValueError with a descriptive error message

    def _process_task(self, task):
        _, data_raw = task
        data = json.loads(data_raw)
        self.detail_url = data['detail_url']
        self.question_id = data['question_id']
        self.title = data['title']
        
        logging.critical(f"fenggen redis question_id={self.question_id} title={self.title} detail_url={self.detail_url}")
        sys.stdout.flush()
        sys.stderr.flush()
        
        # 返回一个请求对象，而不是直接yield
        return scrapy.Request(
            url=self.detail_url,
            callback=self.parse,
            errback=self.error_handler,
            dont_filter=True
        )

    def parse(self, response):
        # 处理当前任务
        logging.critical(f"fg parse called for {response.url}")
        sys.stdout.flush()
        sys.stderr.flush()

        if response.status != 200:
            return

        content = response.xpath(self.XPATH_1).get()        
        logging.critical(f"fenggen url={response.url} question_id={self.question_id} content={content}")

        item_postgres = Scrapy3ItemStackOverflow()
        item_postgres['question_id'] = self.question_id
        item_postgres['tag1'] = 1            
        # item_postgres['task_id_c'] = self.task_id_c
        # item_postgres['spider_c'] = self.spider_c
        # item_postgres['ip_c'] = self.ip_c
        # item_postgres['docker_id_c'] = self.docker_id_c
        # item_postgres['worker_id_c'] = self.worker_id_c
        item_postgres['pipetype'] = 'postgres'
        yield item_postgres

        item_mongo = Scrapy3ItemStackOverflow()
        item_mongo['detail'] = content
        item_mongo['question_id'] = self.question_id
        item_mongo['pipetype'] = 'mongo'
        yield item_mongo
        
        # 不再尝试获取新任务，每个Celery任务处理一个URL
        logging.critical(f"Task completed for question_id={self.question_id}")

    def check_queue(self, response):
        # 再次检查队列
        task = self.redis_conn.brpop('detail_url_queue', timeout=5)
        if task:
            yield self._process_task(task)
        else:
            # 如果仍然没有任务，可以再次调度检查
            yield scrapy.Request(
                url=self.url_virtual,
                callback=self.check_queue,
                dont_filter=True,
                priority=-100,
                meta={"wait": True}
            )    

    def error_handler(self, failure):
        # 处理请求错误
        logging.critical(f"Request failed: {failure}")
        sys.stdout.flush()
        sys.stderr.flush()
        # 尝试获取下一个任务
        task = self.redis_conn.brpop('detail_url_queue', timeout=5)
        if task:
            yield self._process_task(task)
        

        






        

            
        

        