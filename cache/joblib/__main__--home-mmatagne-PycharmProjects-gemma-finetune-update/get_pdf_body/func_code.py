# first line: 163
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
