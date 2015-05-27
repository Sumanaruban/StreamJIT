# May 27, 2015
import deps  # fix sys.path
import opentuner

from opentuner.measurement import MeasurementDriver


class StreamJITMD(MeasurementDriver):

    def __init__(self, **kwargs):
        super(StreamJITMD, self).__init__(**kwargs)

    def process_all(self):
        print "process_all..."
        super(StreamJITMD, self).process_all()

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

        self.report_result(desired_result, result, input)