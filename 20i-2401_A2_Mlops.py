from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def save_to_csv(data):
    if data:
        df = pd.DataFrame(data)
        df.to_csv('/mnt/c/Users/iqraz/Documents/airflow/dags/data/dawnData.csv', index=False)
    else:
        logging.info("No data to save.")

def extract_and_transform(**kwargs):
    url = kwargs['url']
    source = kwargs['source']
    selector = kwargs['selector']
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504, 524])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        response = session.get(url, timeout=20)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = [a['href'] for a in soup.select(selector) if 'href' in a.attrs]
        links = [url + '/' + link.lstrip('/') if not link.startswith('http') else link for link in links]
        data = []
        for link in links:
            try:
                response = session.get(link, timeout=20)
                article_soup = BeautifulSoup(response.text, 'html.parser')
                title_element = article_soup.find('title')
                title = title_element.text.strip() if title_element else None
                paragraphs = article_soup.find_all('p')
                description = ' '.join(p.text.strip() for p in paragraphs if p.text.strip()) if paragraphs else None

                if title and description:
                    title = re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', title)).strip()
                    description = re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', description)).strip()
                    data.append({
                        'title': title,
                        'description': description,
                        'source': source,
                        'url': link
                    })
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to fetch details from {link}: {str(e)}")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch {url}: {str(e)}")
        return []

dag = DAG('pipeline_of_mlops', start_date=datetime(2024, 5, 10), schedule="@daily")

with dag:
    source = 'Dawn'
    info = {'url': 'https://www.dawn.com', 'selector': 'article.story a.story__link'}

    init_repo_and_dvc = BashOperator(
        task_id='initialize_of_git_dvc',
        bash_command="""
        cd /mnt/c/Users/iqraz/Documents/airflow/dags &&
        if [ ! -d ".git" ]; then
            git init &&
            git remote add origin https://github.com/IQRAZAFAR927/20i-2401_Assignment-2_mlops.git ;
        fi &&
        if [ ! -d ".dvc" ]; then
            dvc init &&
            dvc remote add -d myremote gdrive://1wWC71v0RHF7_3huzLXBp5-iS0GRC-Cnq;
        fi
        """
    )
    content_extraction = PythonOperator(
        task_id='extract_content_dvc',
        python_callable=extract_and_transform,
        op_kwargs=info
    )
    persist_data = PythonOperator(
        task_id='persist_scraped_data_mlops',
        python_callable=save_to_csv,
        op_kwargs={'data': content_extraction.output},
    )
    version_control_data = BashOperator(
        task_id='version_control_with_dvc_and_git_mlops',
        bash_command="""
        cd /mnt/c/Users/iqraz/Documents/airflow/dags &&
        dvc add data/dawnData.csv &&
        dvc push &&
        git add data/dawnData.csv.dvc data/.gitignore &&
        git commit -m 'Updated data files' &&
        git push origin master
        """
    )

    init_repo_and_dvc >> content_extraction >> persist_data >> version_control_data