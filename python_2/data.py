from sys import argv
import re
script, from_file = argv

web = {}
user = {}
input = open(from_file, 'r')
line = input.readline()
while line != '':
	p = re.match(r'^/', line)
	if p:
		site = line.split()
		web[site[0]] = int(site[1])
	q = re.match(r'^\d', line)
	if q:
		ip = line.split()
		user[ip[0]] = int(ip[1])
	line = input.readline()
print("*****number of unique users*****")
print(len(user))
print("**********")
print("*****number of page views*****")
print(len(web))
print("**********")
user_sort = sorted(user.items(), key=lambda asd:asd[1], reverse = True)
web_sort = sorted(web.items(), key=lambda asd:asd[1], reverse = True)
print(user_sort)
print(web_sort)
#print(web)



