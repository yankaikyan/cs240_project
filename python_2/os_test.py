import os

for root, dirs, files in os.walk(".", topdown=False):
	for name in files:
		print (os.path.join(root, name))
#a = os.walk('E:\python\*.html')
