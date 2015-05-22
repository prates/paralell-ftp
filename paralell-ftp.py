from ftplib import FTP
from threading import Thread
import os

class paralell_ftp:
	

	def connect(self, server):
		conn=FTP(server)
		conn.login()
		return conn

	def cd(self, conn, path):
		conn.cwd(path)


	def list(self, conn):
		return conn.nlst()

	def get(self, data, conn):
		if type(data)==str: 
			conn.retrbinary("RETR " + data ,open(data, 'wb').write)
		elif type(data)==list:
			for i in data:
				conn.retrbinary("RETR " + i ,open(i, 'wb').write)

			


	def makeGrupos(self, data, ngrupos):
		grupos={}
		for i in range(len(data)):
			if not grupos.__contains__(i%ngrupos):
				grupos[i%ngrupos]=[data[i]]
			else:
				grupos[i%ngrupos].append(data[i])
		return grupos

	def quit(self, conn):
		self.conn.quit()

	def exec_n_thread(self, url, path, files):
		conn=self.connect(url)
		conn.cwd(path)
		self.get(files, conn)
		self.quit(conn)


	def reduce_list(self, listafiles):
		for i in os.listdir('.'):
			if listafiles.__contains__(i):
				listafiles.remove(i)
		return listafiles	

	def get_all(self, url, path, path_local, num_thread):
		conn=self.connect(url)
		self.cd(conn,path)
		files=self.list(conn)
		os.chdir(path_local)
		files=self.reduce_list(files)
		conn.quit()
		th=[]
		grupos=self.makeGrupos(files, num_thread)
		for i in grupos.items():
			t1=Thread(target=self.exec_n_thread, args=(url, path, i[1],))
			th.append(t1)
		for i in th:
			i.start()



import sys

if len(sys.argv)==2:
	if  sys.argv[1]=='--help':
		print "FTP multi-thead \n python paralell-ftp.py endereco_web diretorio_server diretorio_local numero_threads"

elif len(sys.argv)==5:
	endereco=sys.argv[1]
	diretorio_server=sys.argv[2]
	diretorio_local=sys.argv[3]
	worker=int(sys.argv[4])
	ftp=paralell_ftp()
	ftp.get_all(endereco, diretorio_server, diretorio_local, worker)
	#ftp.get_all('ftp.bmf.com.br', 'MarketData/Bovespa-Opcoes/', 10)

else:
	print "Use: \n python paralell-ftp.py --help"
