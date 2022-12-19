import os
import sys
import glob
import time
import zlib
import pathlib
try:
	from tabulate import tabulate
except:
	print('Missing dependency. Install with: pip install tabulate')
	quit()
try:
	from kaitaistruct import KaitaiStream
except:
	print('Missing dependency. Install with: pip install kaitaistruct')
	quit()

####################### Kaitai Struct .BAK format definition block #######################

from pkg_resources import parse_version
from kaitaistruct import __version__ as ks_version, KaitaiStruct, KaitaiStream, BytesIO
from enum import Enum


if parse_version(ks_version) < parse_version('0.7'):
	raise Exception("Incompatible Kaitai Struct Python API: 0.7 or later is required, but you have %s" % (ks_version))

class Exbaktor(KaitaiStruct):

	class ObjectType(Enum):
		directory = 68
		file = 70
	def __init__(self, _io, _parent=None, _root=None):
		self._io = _io
		self._parent = _parent
		self._root = _root if _root else self
		self._read()

	def _read(self):
		self.root_folders = self._root.RootFolder(self._io, self, self._root)
		self.bak_objects = []
		i = 0
		while True:
			_ = self._root.BakObject(self._io, self, self._root)
			self.bak_objects.append(_)
			if _.end_byte == b"\x45":
				break
			i += 1

	class RootFolder(KaitaiStruct):
		def __init__(self, _io, _parent=None, _root=None):
			self._io = _io
			self._parent = _parent
			self._root = _root if _root else self
			self._read()

		def _read(self):
			self.object_type = self._root.ObjectType(self._io.read_u1())
			self.root_folder_name_length = self._io.read_u2be()
			self.root_folder_name = (self._io.read_bytes(self.root_folder_name_length)).decode(u"UTF-8")
			self.spacer_1 = self._io.ensure_fixed_contents(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")


	class BakObject(KaitaiStruct):
		def __init__(self, _io, _parent=None, _root=None):
			self._io = _io
			self._parent = _parent
			self._root = _root if _root else self
			self._read()

		def _read(self):
			if  ((self.next_byte != b"\x44") and (self.next_byte != b"\x46") and ( ((self.dir_spec == b"\x01\x44") or (self.dir_spec == b"\x01\x46")) )) :
				self.folder_id_start = self._io.read_bytes(3)

			if self.next_byte2 == b"\x01":
				self.divider = self._io.ensure_fixed_contents(b"\x01")

			self.object_type = self._root.ObjectType(self._io.read_u1())
			self.name_length = self._io.read_u2be()
			self.name = (self._io.read_bytes(self.name_length)).decode(u"UTF-8")
			self.spacer_1 = self._io.ensure_fixed_contents(b"\x00\x00\x00\x00\x00")
			self.folder_id_end = self._io.read_bytes(3)
			if self.object_type == self._root.ObjectType.directory:
				self.spacer_2 = self._io.ensure_fixed_contents(b"\x00\x00\x00\x00\x00")

			if self.object_type == self._root.ObjectType.file:
				self.file_size = self._io.read_u8be()

			if self.object_type == self._root.ObjectType.file:
				self.spacer_3 = self._io.ensure_fixed_contents(b"\x00\x00")

			if self.object_type == self._root.ObjectType.file:
				self.timestamp = self._io.read_bytes(6)

			if self.object_type == self._root.ObjectType.file:
				self.divider_2 = self._io.ensure_fixed_contents(b"\x01")

			if self.object_type == self._root.ObjectType.file:
				self.file_contents = self._io.read_bytes(self.file_size)


		@property
		def next_byte(self):
			if hasattr(self, '_m_next_byte'):
				return self._m_next_byte if hasattr(self, '_m_next_byte') else None

			_pos = self._io.pos()
			self._io.seek(self._io.pos())
			self._m_next_byte = self._io.read_bytes(1)
			self._io.seek(_pos)
			return self._m_next_byte if hasattr(self, '_m_next_byte') else None

		@property
		def next_byte2(self):
			if hasattr(self, '_m_next_byte2'):
				return self._m_next_byte2 if hasattr(self, '_m_next_byte2') else None

			_pos = self._io.pos()
			self._io.seek(self._io.pos())
			self._m_next_byte2 = self._io.read_bytes(1)
			self._io.seek(_pos)
			return self._m_next_byte2 if hasattr(self, '_m_next_byte2') else None

		@property
		def dir_spec(self):
			if hasattr(self, '_m_dir_spec'):
				return self._m_dir_spec if hasattr(self, '_m_dir_spec') else None

			_pos = self._io.pos()
			self._io.seek((self._io.pos() + 3))
			self._m_dir_spec = self._io.read_bytes(2)
			self._io.seek(_pos)
			return self._m_dir_spec if hasattr(self, '_m_dir_spec') else None

		@property
		def end_byte(self):
			if hasattr(self, '_m_end_byte'):
				return self._m_end_byte if hasattr(self, '_m_end_byte') else None

			_pos = self._io.pos()
			self._io.seek(self._io.pos())
			self._m_end_byte = self._io.read_bytes(1)
			self._io.seek(_pos)
			return self._m_end_byte if hasattr(self, '_m_end_byte') else None

##########################################################################################

try:
	folder_path = sys.argv[1]
except:
	print('Argument missing - provide a path to a folder with BAK files')
	quit()
abs_folder_path = os.path.abspath(folder_path)
if os.path.exists(abs_folder_path) == False:
    print('Provided path does not exist')
    quit() 

print('')
print('Clearwell backup extractor      /$$$$$$$   /$$$$$$  /$$   /$$')
print('                               | $$__  $$ /$$__  $$| $$  /$$/')
print('             /$$$$$$  /$$   /$$| $$  \ $$| $$  \ $$| $$ /$$/ ')
print('            /$$__  $$|  $$ /$$/| $$$$$$$ | $$$$$$$$| $$$$$/  ')
print('           | $$$$$$$$ \  $$$$/ | $$__  $$| $$__  $$| $$  $$  ')
print('           | $$_____/  >$$  $$ | $$  \ $$| $$  | $$| $$\  $$ ')
print('           |  $$$$$$$ /$$/\  $$| $$$$$$$/| $$  | $$| $$ \  $$')
print('            \_______/|__/  \__/|_______/ |__/  |__/|__/  \__/')
print('')
print('')
time.sleep(3)

stats_headers = ['BAK file', 'File Count', 'File Volume [MB]']
stats = []

for file in glob.iglob(abs_folder_path + '**/**', recursive = True):
	filename = os.path.basename(file)
	if filename.endswith('.bak'):
		if 'BackupSet' in filename:
			data_folder = 'data'
		elif 'BackupExtSet' in filename:
			data_folder = 'extdata'
		
		print('Processing ' + filename)
		with open(file, 'rb') as f:
			hexdata = f.read().hex()
			
		deflated_hex = hexdata[hexdata.find('2e62616b7801') + 12:] # remove .bak file header (everything up to '.bakx.' in hex)
		deflated_bytes = bytes.fromhex(deflated_hex)
		inflated_bytes = zlib.decompress(deflated_bytes, -15) # deflate the rest of the file in the 'no headers' mode
		stream = Exbaktor.from_bytes(inflated_bytes)
		
		root_folder = os.path.join(abs_folder_path, data_folder, 'esadb', stream.root_folders.root_folder_name)
		pathlib.Path(root_folder).mkdir(parents = True, exist_ok = True)
		
		file_count = 0
		file_volume = 0
		latest_folder = root_folder
		
		folder_structure = {}
		folder_structure['000000'] = root_folder
		
		for bak_object in stream.bak_objects:
			if hasattr(bak_object, 'folder_id_start') and bak_object.folder_id_end.hex() not in folder_structure: # handles nodes that are starting a new folder level
				folder_structure[bak_object.folder_id_start.hex()] = latest_folder
				if bak_object.object_type == Exbaktor.ObjectType.directory: # if the node is a folder
					if not os.path.isdir(os.path.join(latest_folder, bak_object.name)):
						print('	Creating directory ' + bak_object.name)
						os.mkdir(os.path.join(latest_folder, bak_object.name))
					else:
						print('	Directory ' + bak_object.name + ' already exists. Skipping.')
					latest_folder = os.path.join(latest_folder, bak_object.name)
				elif bak_object.object_type == Exbaktor.ObjectType.file: # if the node is a file
					print('	Extracting file ' + bak_object.name + ' (' + str(f'{bak_object.file_size:,}') + ' bytes)')
					file_path = os.path.join(latest_folder, bak_object.name)
					with open(file_path, 'wb') as f:
						f.write(bak_object.file_contents)
					timestamp = int(round(int(bak_object.timestamp.hex(), 16)/1000))
					if (timestamp > 946684800) and (timestamp < int(time.time())): # restore only sensible timestamps from 2000-01-01 onwards
						os.utime(file_path,(timestamp, timestamp))
					file_count += 1
					file_volume += bak_object.file_size/1024/1024 # sticking to a wrong, but Microsoft-aligned MB calculation
			else: # handles nodes that should be extracted to an existing folder level
				folder_path = folder_structure[bak_object.folder_id_end.hex()]
				if bak_object.object_type == Exbaktor.ObjectType.directory: # if the node is a folder
					if not os.path.isdir(os.path.join(folder_path, bak_object.name)):
						print('	Creating directory ' + bak_object.name)
						os.mkdir(os.path.join(folder_path, bak_object.name))
					else:
						print('	Directory ' + bak_object.name + ' already exists. Skipping.')
					latest_folder = os.path.join(folder_path, bak_object.name)
				elif bak_object.object_type == Exbaktor.ObjectType.file: # if the node is a file
					print('	Extracting file ' + bak_object.name + ' (' + str(f'{bak_object.file_size:,}') + ' bytes)')
					file_path = os.path.join(folder_path, bak_object.name)
					with open(file_path, 'wb') as f:
						f.write(bak_object.file_contents)
						
					timestamp = int(round(int(bak_object.timestamp.hex(), 16)/1000))
					if (timestamp > 946684800) and (timestamp < int(time.time())): # restore only sensible timestamps from 2000-01-01 onwards
						os.utime(file_path,(timestamp, timestamp))
					file_count += 1
					file_volume += bak_object.file_size/1024/1024
		stats.append([filename, '{:,.0f}'.format(file_count), '{:,.2f}'.format(file_volume)])
		stream.close()

total_count = sum(int(v[1].replace(',', '')) for v in stats)
total_volume = sum(float(v[2].replace(',', '')) for v in stats)
stats.append(['-------------------------', '------------', '------------------'])
stats.append(['Total', '{:,.0f}'.format(total_count), '{:,.2f}'.format(total_volume)])
print('')
print(tabulate(stats, headers = stats_headers, tablefmt='psql', colalign=('left', 'right', 'right')))
