__author__ = 'Iqbal Haidee'

from datetime import datetime
import traceback
import wikipedia
import json
import csv
import re

# ==============================================================
#                      Wikipedia Crawler
# ==============================================================

#   ----------- place url at wikipedia.csv file -----------

class WikipediaCrawler:
    def start(self):
        try: 
            print('Start Crawling Process...')
            with open('wikipedia.csv', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, ['url'], delimiter=';')
                for item in reader:
                    print(f"Crawling {item['url']}")
                    content = self.content_crawler(item=item)
                    link = item['url']
                    title = link.split('/')[4].replace('_', ' ')
                    source = link.split('/')[2].split('.')[1]
                    domain = link.split('/')[2]
                    crawling_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    crawling_epoch = int(datetime.now().timestamp())
                    
                    raw_data = {
                        'link': link,
                        'domain': domain,
                        'source': source,
                        'crawling_time': crawling_time,
                        'crawling_epoch': crawling_epoch,
                        'title': title, 
                        'article': content,
                    }
                    
                    file_name = f'{crawling_epoch}.json'
                    
                    with open(f'output/{file_name}', 'w') as f:
                        json.dump(raw_data, f)
                    
            print('Crawling done, waiting for another job...')
            
        except Exception as e:
            traceback.print_exc()
            print(f'failed to crawl: {e}')

    def content_crawler(self, item):
        try:
            lang = item['url'].split('/')[2].split('.')[0]
            wikipedia.set_lang(lang)
            title = item['url'].split('/')[4].replace('_', ' ')
            page = wikipedia.page(title=title, auto_suggest=False)
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
                content=contents,
                media=medias
            )
            
            return data

        except Exception as e:
            traceback.print_exc()
            print(f'failed to crawl content: {e}')

if __name__ == '__main__':
    WikipediaCrawler().start()
