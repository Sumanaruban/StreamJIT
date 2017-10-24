# May 27, 2015
import sys

import deps  # fix sys.path
import opentuner

from opentuner.measurement import MeasurementDriver


class StreamJITMD(MeasurementDriver):

    def __init__(self, **kwargs):
        super(StreamJITMD, self).__init__(**kwargs)
        self.pendingResults={}
        self.parallel_cfgs = 2

    #Copied from MeasurementDriver.process_all()
    def process_all(self):
        """ process all desired_results in the database"""
        self.lap_timer()  # reset timer
        q = self.query_pending_desired_results()

        if self.interface.parallel_compile:
          #This shouldn't happen in StreamJIT case. I just deleted code that handles this case. Sumanan. 27-May-2015.
          raise RuntimeError("This shouldn't happen in StreamJIT case")
        else:
          for dr in q.all():
            if self.claim_desired_result(dr):
              if len(self.pendingResults) > self.parallel_cfgs:
                  raise RuntimeError("len(self.pendingResults) > self.parallel_cfgs. This shouldn't happen")
              elif len(self.pendingResults) == self.parallel_cfgs:
                  self.update_result()
              self.run_desired_result(dr)

    #Copied from MeasurementDriver.run_desired_result()
    def run_desired_result(self, desired_result, compile_result=None,
                         exec_id=None):
        """
    create a new Result using input manager and measurment interface
    Optional compile_result paramater can be passed to run_precompiled as
    the return value of compile()
    Optional exec_id paramater can be passed to run_precompiled in case of
    locating a specific executable
    """
        desired_result.limit = self.run_time_limit(desired_result)

        input = self.input_manager.select_input(desired_result)
        self.session.add(input)
        self.session.flush()

        opentuner.measurement.driver.log.debug('running desired result %s on input %s', desired_result.id,
              input.id)

        self.input_manager.before_run(desired_result, input)

        result = self.interface.run_precompiled(desired_result, input,
                                            desired_result.limit,
                                            compile_result, exec_id)
        self.pendingResults[desired_result.id] = desired_result,input


    def update_result(self):
        desired_result_id, result = self.interface.wait_for_result()
        if self.pendingResults.has_key(desired_result_id):
            desired_result, input = self.pendingResults[desired_result_id]
        else:
            raise RuntimeError("Unknown desired_result_id %d"%desired_result_id)
        del self.pendingResults[desired_result_id]
        self.report_result(desired_result, result, input)

    def tuning_completed(self):
        while len(self.pendingResults) > 0:
            self.update_result()
        self.interface.connection.close()
        sys.exit(0)