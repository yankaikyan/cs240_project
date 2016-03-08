from bs4 import BeautifulSoup

soup = BeautifulSoup(open("xxx.html", encoding="utf8"), "lxml")
print (soup)
print (soup.get_text())
