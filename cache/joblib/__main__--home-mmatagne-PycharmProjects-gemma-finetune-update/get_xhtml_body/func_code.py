# first line: 177
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
