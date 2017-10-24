#!../../venv/bin/python
import argparse
import logging
import json
import sjparameters
import configuration
import streamjit
import sys
import signal
import os

import deps #fix sys.path
import opentuner
from opentuner.search.manipulator import (ConfigurationManipulator, IntegerParameter, FloatParameter)
from opentuner.measurement import MeasurementInterface
from opentuner.measurement.inputmanager import FixedInputManager
from opentuner.tuningrunmain import TuningRunMain
from opentuner.search.objective import MinimizeTime
from streamjitmd import StreamJITMD

class StreamJitMI(MeasurementInterface):
	''' Measurement Interface for tunning a StreamJit application'''
	def __init__(self, args, configuration, connection, manipulator, inputmanager, objective):
		super(StreamJitMI, self).__init__(args = args, program_name = args.program, manipulator = manipulator, input_manager = inputmanager, objective = objective)
		self.connection = connection
		self.trycount = 0
		self.config = configuration
		signal.signal(signal.SIGUSR1, self.receive_signal)
		print 'My PID is:', os.getpid()
		self.submitted_desired_result={}

	def run(self, desired_result, input, limit):
		self.trycount = self.trycount + 1
		#print self.trycount

		cfg_data = desired_result.configuration.data
		#self.niceprint(cfg_data)
		for k in self.config.params:
			self.config.getParameter(k).update_value_for_json(cfg_data)
		self.config.put_extra_data("configPrefix", str(self.trycount), "java.lang.String")
		self.connection.sendmsg(self.config.toJSON())
		self.submitted_desired_result[self.trycount]=desired_result.id

	def niceprint(self, cfg):
		print "\n--------------------------------------------------"
		print self.trycount
		for key in cfg.keys():
			print "%s - %s"%(key, cfg[key])

	def program_name(self):
		return self.args.program

	def program_version(self):
		return "1.0"

	def save_final_config(self, configuration):
		'''called at the end of autotuning with the best resultsdb.models.Configuration'''
		cfg_data = configuration.data
		#print "Final configuration", cfg_data
		for k in self.config.params:
			self.config.getParameter(k).update_value_for_json(cfg_data)

		self.config.put_extra_data("configPrefix", "final", "java.lang.String")
		self.connection.sendmsg("Completed")
		self.connection.sendmsg(self.config.toJSON())
		self.tuningrunmain.measurement_driver.tuning_completed()


	def receive_signal(self, signum, stack):
		print 'Received Signal:', signum
		self.exit()

	#[19-03-2015] Commits the current status to the database and exists safely.
	def exit(self):
		#data = raw_input ( "exit cmd received. Press Keyboard to exit..." )
		self.tuningrunmain.search_driver.args.test_limit=self.tuningrunmain.search_driver.test_count - 1
		#self.connection.close()
		#sys.exit(1)

	def tuning_run_main(self, trm):
		self.tuningrunmain=trm

	def wait_for_result(self):
		msg = self.connection.recvmsg()
		if (msg == "exit\n"):
			#data = raw_input ( "exit cmd received. Press Keyboard to exit..." )
			self.connection.close()
			sys.exit(1)

		pair = msg.split(':')
		if len(pair) != 2:
			raise RuntimeError('''Time must be reported in "configPrefix:time" format.''')
		else:
			configPrefix = int(pair[0])
			exetime = float(pair[1])

		if self.submitted_desired_result.has_key(configPrefix):
			desired_result_id = self.submitted_desired_result[configPrefix]
			del self.submitted_desired_result[configPrefix]
		else:
			raise RuntimeError("Unknown configuration %d"%configPrefix)

		if exetime < 0:
			print "Error in configuration %d"%configPrefix
			return desired_result_id, opentuner.resultsdb.models.Result(state='ERROR', time=float('inf'))
		else:
			print "Execution time of configuration %d is %fms"%(configPrefix, exetime)
			return desired_result_id, opentuner.resultsdb.models.Result(time=exetime)

def main(args, cfg, connection):
	logging.basicConfig(level=logging.INFO)
	manipulator = ConfigurationManipulator()

	#print "\nFeature variables...."
	for p in cfg.getAllParameters().values():
		manipulator.add_parameter(p)
	
	mi = StreamJitMI(args, cfg, connection, manipulator, FixedInputManager(),
                    MinimizeTime())

	m = TuningRunMain(mi, args, measurement_driver=StreamJITMD)
	mi.tuning_run_main(m)
	m.main()

def start(argv, cfg, connection):
	log = logging.getLogger(__name__)
	parser = argparse.ArgumentParser(parents=opentuner.argparsers())

	parser.add_argument('--program', help='Name of the StreamJit application')
	
	args = parser.parse_args(argv)

	print 'test_limit=%d'%args.test_limit

	if not args.database:
		args.database = 'sqlite:///' + args.program.rstrip('\n') + '.db'

	main(args, cfg, connection)
