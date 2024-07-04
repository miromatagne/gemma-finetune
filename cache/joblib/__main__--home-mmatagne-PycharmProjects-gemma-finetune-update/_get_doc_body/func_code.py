# first line: 223
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
