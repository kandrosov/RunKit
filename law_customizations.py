import copy
import law
import luigi
import math
import os

law.contrib.load("htcondor")

def copy_param(ref_param, new_default):
  param = copy.deepcopy(ref_param)
  param._default = new_default
  return param

def get_param_value(cls, param_name):
    param = getattr(cls, param_name)
    return param.task_value(cls.__name__, param_name)

class HTCondorWorkflow(law.htcondor.HTCondorWorkflow):
    max_runtime = law.DurationParameter(default=24.0, unit="h", significant=False,
                                        description="maximum runtime, default unit is hours")
    n_cpus = luigi.IntParameter(default=1, description="number of cpus")
    poll_interval = copy_param(law.htcondor.HTCondorWorkflow.poll_interval, 2)
    requirements = luigi.Parameter(default='')
    bootstrap_path = luigi.Parameter()
    log_path = luigi.Parameter(default='')
    sub_dir = luigi.Parameter()

    def htcondor_output_directory(self):
        # the directory where submission meta data should be stored
        return law.LocalDirectoryTarget(os.path.join(self.sub_dir, self.__class__.__name__))

    def htcondor_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        return self.bootstrap_path

    def htcondor_job_config(self, config, job_num, branches):
        config.render_variables["analysis_path"] = os.getenv("ANALYSIS_PATH")
        if len(self.requirements) > 0:
            config.custom_content.append(("requirements", self.requirements))
        config.custom_content.append(("+MaxRuntime", int(math.floor(self.max_runtime * 3600)) - 1))
        config.custom_content.append(("RequestCpus", self.n_cpus))

        if len(self.log_path) > 0:
            log_path = os.path.abspath(self.log_path)
            os.makedirs(log_path, exist_ok=True)
            config.custom_content.append(("log", os.path.join(log_path, 'job.$(ClusterId).$(ProcId).log')))
            config.custom_content.append(("output", os.path.join(log_path, 'job.$(ClusterId).$(ProcId).out')))
            config.custom_content.append(("error", os.path.join(log_path, 'job.$(ClusterId).$(ProcId).err')))
        return config