from requests_html import HTMLSession
session = HTMLSession()

r = session.get('https://www.capterra.com/p/212826/Google-Pay/reviews/')
print(r.status_code)