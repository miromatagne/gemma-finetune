"""
Cellar text and eurovoc extraction
	python dataset_creator.py 10000 dataset.jsonl
will extract for the last 10,000 days text in english and eurovoc labels in the JSON line file.
requirements:
beautifulsoup4==4.12.2
docx2txt==0.8
ipython==8.14.0
jinja2==3.1.2
joblib==1.3.1
pdfminer.six==20221105
pip-chill==1.0.3
pycryptodome==3.18.0
requests==2.31.0
tqdm==4.65.0
xmltodict==0.13.0
"""
import datetime
import json
from concurrent.futures import ProcessPoolExecutor

from bs4 import BeautifulSoup
import logging
import re
import sys

from tqdm import tqdm
from io import BytesIO
import jinja2
from joblib import Memory

location = './cache'
memory = Memory(location, verbose=0)

log = logging.getLogger(__name__)
log.addHandler(logging.FileHandler('collect.log'))
log.setLevel(logging.DEBUG)

import xmltodict

import docx2txt as docx2txt
import requests
from joblib import expires_after
from pdfminer.high_level import extract_text

user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'


def clean_text(func):
    """
    Decorator used to clean the text
    :param func:
    :return:
    """

    def inner(*args, **kwargs):
        text = func(*args, **kwargs)
        text = text.replace("\n", " ")
        text = text.replace(" .", ".")
        text = re.sub(' +', ' ', text)
        text = re.sub(' *[.] *', '. ', text)
        text = re.sub('\.\s*\.\s*\.+', '. ', text)
        text = '. '.join([s.strip() for s in text.split(".") if len(s.strip())])
        return text

    return inner


@memory.cache(cache_validation_callback=expires_after(minutes=120))
def get_eurovoc_terms_and_id():
    eurovoc_terms_and_id = {}
    response = requests.get('http://publications.europa.eu/resource/dataset/eurovoc',
                            headers={'Accept': 'application/xml',
                                     'Accept-Language': 'en',
                                     'User-Agent': user_agent
                                     }
                            )
    data = xmltodict.parse(response.content)
    for term in data['xs:schema']['xs:simpleType']['xs:restriction']['xs:enumeration']:
        try:
            name = term['xs:annotation']['xs:documentation'].split('/')[0].strip()
            for r in term['xs:annotation']['xs:appinfo']['record']:
                if r['@thesaurus_id'] != '':
                    eurovoc_terms_and_id[name.lower()] = r['@thesaurus_id']
        except KeyError as e:
            log.warning("⚠️ Could not parse", term)
    return eurovoc_terms_and_id


def get_sparql_query(d):
    start = d.strftime('%Y-%m-%d')
    end = d + datetime.timedelta(days=2)
    end = end.strftime('%Y-%m-%d')
    environment = jinja2.Environment()
    template = environment.from_string(open("query.j2", 'r').read())
    return template.render(start=start, end=end)


def get_json_response(d):
    url = "https://publications.europa.eu/webapi/rdf/sparql"
    headers = {'User-Agent': user_agent}
    params = {"default-graph-uri": "",
              "query": get_sparql_query(d),
              "format": "application/sparql-results+json",
              "timeout": "0",
              "debug": "on",
              "run": "Run Query"}

    response = requests.get(url, headers=headers, params=params)
    assert response.status_code == 200
    return response.json()


def get_concepts_id(list_of_eurovoc_terms):
    terms = get_eurovoc_terms_and_id()
    for e in list_of_eurovoc_terms:
        try:
            yield terms[e.strip().lower()]
        except KeyError:
            log.warning(f"⚠️ Could not find {e} in Eurovoc")


def get_docs(d):
    results = get_json_response(d)
    for r in results['results']['bindings']:
        terms = r['subjects']['value'].replace(u'\xa0', u' ').split(',')
        r['eurovoc_concepts'] = terms  # list(get_concepts_id(terms))
        r['url'] = r['cellarURIs']['value']
        r['title'] = r['title']['value']
        r['date'] = r['date']['value']
        r['lang'] = r['langIdentifier']['value'].lower()
        r['formats'] = [t for t in r['mtypes']['value'].split(',')]
        for c in ['cellarURIs', 'mtypes', 'langIdentifier', 'subjects', 'authors', 'workTypes', 'workIds']:
            del r[c]
        yield r


def get_docs_text(d):
    docs = list(get_docs(d))
    print(f"Processing documents ... {len(docs)}")
    with ProcessPoolExecutor(max_workers=16) as executor:
        for v in tqdm(executor.map(get_body, docs), total=len(docs), colour='green'):
            yield v


def get_body(r):
    try:
        if 'pdf' in r['formats']:
            r['text'] = get_pdf_body(r)
        elif 'docx' in r['formats']:
            r['text'] = get_docx_body(r)
        elif 'doc' in r['formats']:
            r['text'] = get_doc_body(r)
        elif 'xhtml' in r['formats']:
            r['text'] = get_xhtml_body(r)
        else:
            log.warning(f"⚠️ Could not find a parser for {r['formats']}")
        return r
    except Exception as e:
        log.error(str(e) + str(r))


@clean_text
@memory.cache()
def get_pdf_body(r):
    url = r['url']
    language = r['lang']
    accept = 'application/pdf'
    response = requests.get(url, headers={'Accept': accept, 'Accept-Language': language, 'User-Agent': user_agent})
    if response.status_code == 300:
        return " ".join(_multiple_choice(get_pdf_body, response, accept, language))
    elif response.status_code == 200:
        mem = BytesIO(response.content)
        return extract_text(mem)


@clean_text
@memory.cache()
def get_xhtml_body(r):
    url = r['url']
    language = r['lang']
    accept = 'application/xhtml+xml'
    response = requests.get(url, headers={'Accept': accept, 'Accept-Language': language, 'User-Agent': user_agent})
    if response.status_code == 300:
        return " ".join(_multiple_choice(get_xhtml_body, response, accept, language))
    elif response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup.get_text()


def get_docx_body(r):
    accept = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml'
    url = r['url']
    lang = r['lang']
    try:
        return _get_doc_body(url, accept, lang)
    except AssertionError as e:
        log.warning(f"⚠️ Could not download {url} {e}")
        print(f"⚠️ Could not download {r} --- {accept} {e}")
        return ""


def get_doc_body(r):
    accept = 'application/msword'
    url = r['url']
    lang = r['lang']
    try:
        return _get_doc_body(url, accept, lang)
    except AssertionError as e:
        log.warning(f"⚠️ Could not download {url} {e}")
        print(f"⚠️ Could not download {r} --- {accept} {e}")
        return ""


def _multiple_choice(func, response, accept, language):
    soup = BeautifulSoup(response.text, 'html.parser')
    for link in soup.find_all('a'):
        if 'href' in link.attrs:
            url = link.attrs['href']
            yield func(url, accept, language)


@clean_text
@memory.cache()
def _get_doc_body(url, accept, language='en'):
    response = requests.get(url, headers={'Accept': accept, 'Accept-Language': language, 'User-Agent': user_agent})
    if response.status_code == 300:
        return " ".join(_multiple_choice(_get_doc_body, response, accept, language))
    elif response.status_code == 200:
        mem = BytesIO(response.content)
        log.info(f"📄 MS Word doc download and parsed {url}")
        return docx2txt.process(mem)
    else:
        raise AssertionError(f"📄 MS Word doc download failed {url} {response.status_code} {response.content}")


if __name__ == '__main__':
    max = int(sys.argv[1])
    ofiles = {}
    for i in range(max):
        d = datetime.date.today() - datetime.timedelta(days=i)
        print(d)
        ym = d.strftime('%Y-%m')
        if ym not in ofiles:
            ofiles[ym] = open(f"data/data_{ym}.jsonl", 'w')
        try:
            for d in get_docs_text(d):
                ofiles[ym].write(json.dumps(d, ensure_ascii=False) + '\n')
                ofiles[ym].flush()
        except Exception as e:
            log.error('Day ' + str(d) + ' ' + str(e))
            print('Day ' + str(d) + ' ' + str(e))
    for f in ofiles.values():
        f.close()
