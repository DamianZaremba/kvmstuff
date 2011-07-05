import os
import libvirt
import rrdtool
import logging
import subprocess
import threading
RRD_STEP = 360 # If you change this you are on your own for fixing any previously created rrdfiles
RRD_XFF = 0.5 # If you change this you are on your own for fixing any previously created rrdfiles
RRD_GRAPH_TIMES = [(3600, 'Hour'), (86400, 'Day'), (604800, 'Week'), (2592000, 'Month'), (31536000, 'Year')]
DATASTORE_PATH = "databases/disk/"
GRAPH_PATH = "graphs/disk/"

class grapher(threading.Thread):
	def __init__(self, logger, host, vm):
		self.logger = logger
		self.host = host
		self.vm = vm
		self.vm_escaped = self.vm.replace("/", "").replace("\\", "")
		threading.Thread.__init__(self, name=self.vm)

	def gen_disk_path(self, disk):
		self.logger.debug("gen_disk_path called with %s" % disk)
		disk = disk.replace("/", "-").replace("\\", "-")
		if disk[0] == "-": disk = disk[1:]
		self.logger.debug("gen_disk_path returning with %s" % disk)
		return disk

	def create_rrd_db(self, disk):
		self.logger.debug("create_rrd_db called with %s" % disk)
		rrd_path = os.path.join(DATASTORE_PATH, self.vm_escaped, self.gen_disk_path(disk)) + ".rrd"
		rrd_dir = os.path.dirname(rrd_path)

		self.logger.debug("create_rrd_db checking if %s exists" % rrd_dir)
		if not os.path.isdir(rrd_dir):
			self.logger.debug("create_rrd_db creating %s" % rrd_dir)
			os.makedirs(rrd_dir)

		self.logger.debug("create_rrd_db checking if %s exists" % rrd_path)
		if not os.path.isfile(rrd_path):
			self.logger.debug("create_rrd_db creating %s" % rrd_path)

			try:
				rrdtool.create(rrd_path, [
					# Total disk size
					'DS:total_size:GAUGE:600:U:U',
					# Used disk size
					'DS:used_size:GAUGE:600:U:U',
					# Available disk size
					'DS:available_size:GAUGE:600:U:U'
				], [
					# Every 5min for 1day
					'RRA:AVERAGE:%r:%d:%d' % (
						RRD_XFF,
						(360 / RRD_STEP), # (360 x 1) / RRD_STEP
						((86400 / RRD_STEP) / (360 / RRD_STEP)), # ((1 x 86400) / RRD_STEP) / ((360 x 1) / RRD_STEP)
					),
					# Every 1hour for 1month
					'RRA:AVERAGE:%r:%d:%d' % (
						RRD_XFF,
						(3600 / RRD_STEP), # (1 x 3600) / RRD_STEP
						((2592000 / RRD_STEP) / (3600 / RRD_STEP)), # ((1 x 2592000) / RRD_STEP) / ((1 x 3600)) / RRD_STEP)
					),
					# Every 1day for 1year
					'RRA:AVERAGE:%r:%d:%d' % (
						RRD_XFF,
						(86400 / RRD_STEP), # (1 x 86400) / RRD_STEP
						((31536000 / RRD_STEP) / (86400 / RRD_STEP)), # ((1 x 31536000) / RRD_STEP) / ((1 x 86400) / RRD_STEP)
					),
				])
			except Exception, e:
				self.logger.critical("create_rrd_db Could not create %s (%s)" % (rrd_path, e))
				return False

		self.logger.debug("create_rrd_db returning")
		return True

	def update_rrd_db(self, disk, used, available):
		rrd_path = os.path.join(DATASTORE_PATH, self.vm_escaped, self.gen_disk_path(disk)) + ".rrd"

		self.logger.debug("update_rrd_db checking if %s exists" % rrd_path)
		if not os.path.isfile(rrd_path):
			self.logger.debug("update_rrd_db returning due to %s not existing" % rrd_path)
			return False

		self.logger.debug("update_rrd_db calculating total disk size")
		total_size = used + available
		try:
			rrdtool.update(rrd_path, "N:%r:%r:%r" % (total_size, used, available))
		except Exception, e:
			logger.critical("update_rrd_db failed to update %s (%s)" % (rrd_path, e))

		self.logger.debug("update_rrd_db returning")
		return True

	def dump_graphs(self):
		rrd_dir = os.path.join(DATASTORE_PATH, self.vm_escaped)

		self.logger.debug("dump_graphs checking if %s exists" % rrd_dir)
		if not os.path.isdir(rrd_dir):
			self.logger.debug("dump_graphs returning due to %s not existing" % rrd_dir)
			return False

		self.logger.debug("dump_graphs checking for rrd databases")
		for disk in os.listdir(rrd_dir):
			rrd_path = os.path.join(rrd_dir, disk)
			graph_dir = os.path.join(GRAPH_PATH, self.vm_escaped)
			self.logger.debug("dump_graphs found %s" % rrd_path)

			logger.debug("dump_graphs checking if %s exists" % graph_dir)
			if not os.path.isdir(graph_dir):
				logger.debug("dump_graphs creating %s" % graph_dir)
				os.makedirs(graph_dir)

			pdisk = disk
			if pdisk.endswith(".rrd"): pdisk = pdisk[:-4]

			for time in RRD_GRAPH_TIMES:
				graph_path = "%s-%d.png" % (os.path.join(graph_dir, pdisk), time[0])
				self.logger.debug("dump_graphs trying to dump %s" % graph_path)

				try:
					rrdtool.graph(graph_path,
					'--imgformat', 'PNG',
					'--width', '540',
					'--height', '100',
					'--start', "-%d" % time[0],
					'--end', "-1",
					'--vertical-label', 'Size',
					'--title', '%s - %s (%s)' % (vm, disk, time[1]),
					'--lower-limit', '0',
					'DEF:total_size=%s:total_size:AVERAGE' % rrd_path,
					'DEF:used_size=%s:used_size:AVERAGE' % rrd_path,
					'DEF:available_size=%s:available_size:AVERAGE' % rrd_path,
					'AREA:total_size#CCCCCC:Total disk space',
					'LINE2:available_size#33FF33:Available space',
					'LINE2:used_size#FF3300:Used space')
				except Exception, e:
					logger.critical("Failed to dump graph %s (%s)" % (graph_path, e))

		self.logger.debug("dump_graphs returning")
		return True

	def get_data(self):
		self.logger.debug("get_data Running virt-df")
		p = subprocess.Popen(["virt-df", "-d", self.vm, "--csv"], shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		(stdout, stderr) = p.communicate()
		rc = p.returncode

		if rc == 0:
			self.logger.debug("get_data Ruturning stdout")
			return stdout
		else:
			self.logger.critical('get_data Subprocess returned %d (%s)' % (rc, stderr))
			return False

	def run(self):
		self.logger.debug("run called")

		self.logger.debug("run calling get_data")
		disk_data = self.get_data()
		if not disk_data:
			self.logger.error('run Could not get disk data, aborting!')
			return False

		self.logger.debug("run Looping though stdout")
		for line in disk_data.split("\n")[1:]:
			if line == "": continue # We don't line blank lines (last \n causes one)
			self.logger.debug("run Parsing line from stdout")
			(vm, fs, kblocks, used, available, usedperc) = line.split(",")

			self.logger.debug("run Calling create_rrd_db")
			if self.create_rrd_db(fs):
				self.logger.debug("run Calling update_rrd_db")
				self.update_rrd_db(fs, int(used), int(available))

				self.logger.debug("run Calling dump_graphs")
				self.dump_graphs()
			else:
				self.logger.critical("run create_rrd_db failed")
				return False

		self.logger.debug("run returning")
		return True

if __name__=='__main__':
	threads = []

	logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-8s %(threadName)s: %(message)s")
	logger = logging.getLogger('Grapher')
	logger.setLevel(logging.DEBUG)

	logger.info("__main__ trying to connect to host")
	try:
		host = libvirt.open('qemu:///system')
	except:
		logger.critical("Could not connect to host")
		sys.exit(2)

	logger.info("Getting vms on host")
	for vm in host.listDefinedDomains():
		logger.debug("Found %s" % vm)

		logger.debug("Initializing grapher class for %s" % vm)
		thread = grapher(logger, host, vm)

		logger.debug("Appending thread for %s to threads" % vm)
		threads.append(thread)

		logger.debug("Starting grapher thread for %s" % vm)
		thread.start()

	logger.debug("Joining %d threads" % len(threads))
	for thread in threads:
		thread.join()
	logger.debug("Threads joined")

	logger.debug("__main__ dumping out html")
	if os.path.isdir(DATASTORE_PATH):
		if not os.path.isdir(GRAPH_PATH):
			logger.debug("__main__ creating %s" % GRAPH_PATH)
			os.path.makedirs(GRAPH_PATH)

		html_path = os.path.join(GRAPH_PATH, "index.html")
		logger.debug("__main__ writing out to %s" % html_path)
		fh = open(html_path, "w")
		fh.write('''
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<title>Graphy stuff</title>
</head>
<body>
''')

		logger.debug("__main__ looping though vms")
		for vm in os.listdir(DATASTORE_PATH):
			if not os.path.isdir(os.path.join(DATASTORE_PATH, vm)): continue # vms are dirs
			disks = []

			logger.debug("__main__ checking for disks under %s" % vm)
			for rrddb in os.listdir(os.path.join(DATASTORE_PATH, vm)):
				prrddb = rrddb
				if prrddb.endswith(".rrd"): prrddb = prrddb[:-4]

				logger.debug("__main__ adding disk %s" % prrddb)
				disks.append(prrddb)

			if len(disks) > 0:
				logger.debug("__main__ found disks found for %s, writing out" % vm)
				fh.write('<h2>%s</h2>' % vm)
				for disk in disks:
					fh.write('<h4>%s</h4>' % disk)
					fh.write('<div id="%s-%s">' % (vm, disk))
					for time in RRD_GRAPH_TIMES:
						fh.write('<img src="%s/%s-%d.png" alt="%s - %s (%s)" /> ' % (vm, disk, time[0], vm, disk, time[1]))
					fh.write('</div>')
			else:
				logger.debug("__main__ no disks found for %s, skipping" % vm)

		fh.write('''
</body>
</html>
''')

		fh.close()
	else:
		logger.critical("__main__ could not dump html!")
