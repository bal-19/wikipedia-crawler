__author__ = 'Iqbal Haidee'

from s3.connection import Connection
from datetime import datetime
import traceback
import wikipedia
import json
import csv
import re

# ==============================================
        # CRAWLING ARTIKEL RADIKALISME
# ==============================================

class WikipediaCrawler:
    def __init__(self):
        self.s3 = Connection()
        self.path_s3 = f"s3://ai-pipeline-statistics/data/data_raw/artikel_radikalisme" # + /[nama organisasi]/[source]/[format]/[namafile]

    def start(self):
        try: 
            print('Start Crawling Process...')
            with open('wiki.csv', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, ['organization', 'title', 'country', 'url'], delimiter=';')
                for item in reader:
                    print(f"Crawling {item['url']}")
                    content = self.content_crawler(item=item)
                    link = item['url']
                    source = link.split('/')[2].split('.')[1]
                    domain = link.split('/')[2]
                    data_name = 'Artikel Radikalisme'
                    crawling_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    crawling_epoch = int(datetime.now().timestamp())
                    
                    raw_data = {
                        'link': link,
                        'domain': domain,
                        'source': source,
                        'data_name': data_name,
                        'path_data_raw': None,
                        'crawling_time': crawling_time,
                        'crawling_epoch': crawling_epoch,
                        'organization': item['organization'],
                        'country': item['country'],
                        'title': item['title'],
                        'article': content,
                    }
                    
                    file_name = f'{crawling_epoch}.json'
                    
                    s3_path = f"{self.path_s3}/{item['organization'].lower().replace('/', '')}/{source}/json/{file_name}"
                    
                    raw_data['path_data_raw'] = s3_path
                    
                    with open(f'output/{file_name}', 'w') as f:
                        json.dump(raw_data, f)
                        
                    self.s3.upload(rpath=s3_path, lpath=f'output/{file_name}')
                    
            print('Crawling done, waiting for another job...')
            
        except Exception as e:
            traceback.print_exc()
            print(f'failed to crawl: {e}')

    def content_crawler(self, item):
        try:
            lang = item['url'].split('/')[2].split('.')[0]
            wikipedia.set_lang(lang)
            page = wikipedia.page(title=item['title'], auto_suggest=False)
            content = page.content
            sections = [re.sub(r'=+', '', section).strip() for section in re.findall(r'=+ .* =+', content)]
            content_sections = [c.strip() for c in re.split(r'=+ .* =+', content)]
            
            if len(sections) != len(content_sections):
                sections.insert(0, '')
            
            content_with_section = [(sections[i], content_sections[i]) for i in range(0, len(sections))]
            contents = list()
            
            for s, c in content_with_section:
                contents.append(dict(
                    title=s,
                    text=c
                ))

                
            medias = list()
            for i in page.images:
                medias.append(dict(
                    type='image',
                    url=i
                ))
                
            data = dict(
                lang=lang,
                content=contents
            )
            
            return data

        except Exception as e:
            traceback.print_exc()
            print(f'failed to crawl content: {e}')

if __name__ == '__main__':
    WikipediaCrawler().start()
