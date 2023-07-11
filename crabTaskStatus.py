import json
import re
from enum import Enum

class Status(Enum):
  Unknown = -1
  Defined = 0
  Submitted = 1
  Bootstrapped = 2
  TapeRecall = 3
  InProgress = 4
  WaitingForRecovery = 5
  WaitingForLocalRecovery = 6
  CrabFinished = 7
  PostProcessingFinished = 8
  Failed = 9

class StatusOnServer(Enum):
  QUEUED = 1
  TAPERECALL = 2
  SUBMITTED = 3
  KILLED = 4
  SUBMITFAILED = 5
  RESUBMITFAILED = 6

class StatusOnScheduler(Enum):
  SUBMITTED = 1
  FAILED = 2
  FAILED_KILLED = 3
  COMPLETED = 4

class CrabWarningCategory(Enum):
  Unknown = 0
  BlocksSkipped = 1
  ShortRuntime = 2
  LowCpuEfficiency = 3

class CrabFailureCategory(Enum):
  Unknown = 0
  CannotLocateFile = 1

class JobStatus(Enum):
  unsubmitted = 0
  idle = 1
  cooloff = 2
  running = 3
  toRetry = 4
  finished = 5
  failed = 6
  transferring = 7
  killed = 8
  held = 9

class CrabWarning:
  known_warnings = {
    r"Some blocks from dataset '.+' were skipped  because they are only present at blacklisted and/or not-whitelisted sites.": CrabWarningCategory.BlocksSkipped,
    r"the max jobs runtime is less than 30% of the task requested value": CrabWarningCategory.ShortRuntime,
    r"the average jobs CPU efficiency is less than 50%": CrabWarningCategory.LowCpuEfficiency,
  }
  def __init__(self, warning_message):
    self.category = CrabWarningCategory.Unknown
    self.message = warning_message
    for known_warning, category in CrabWarning.known_warnings.items():
      if re.match(known_warning, warning_message):
        self.category = category
        break

class CrabFailure:
  known_messages = {
    r"CRAB server could not get file locations from Rucio\.": CrabFailureCategory.CannotLocateFile,
  }

  def __init__(self, message):
    self.category = CrabFailureCategory.Unknown
    self.message = message
    for known_message, category in CrabFailure.known_messages.items():
      if re.match(known_message, known_message):
        self.category = category
        break

class LogEntryParser:
  @staticmethod
  def Parse(log_lines):
    task_status = CrabTaskStatus()
    log_lines = [ l.replace('\t', '    ') for l in log_lines ]
    task_status.log_lines = ''.join(log_lines)
    n = 0
    N = len(log_lines)
    try:
      while n < N:
        if len(log_lines[n].strip()) == 0:
          n += 1
          continue
        method_found = False
        for parse_key, parse_method in LogEntryParser._parser_dict.items():
          if log_lines[n].startswith(parse_key):
            value = log_lines[n][len(parse_key):].strip()
            method_found = True
            if type(parse_method) == str:
              setattr(task_status, parse_method, value)
              n += 1
            elif parse_method is not None:
              n = parse_method(task_status, log_lines, n, value)
            else:
              n += 1
            break
        if not method_found:
          raise RuntimeError(f'Unknown log line {n} = "{log_lines[n]}".')
      if task_status.status_on_server == StatusOnServer.QUEUED:
        task_status.status = Status.Submitted
      if task_status.status_on_server == StatusOnServer.TAPERECALL:
        task_status.status = Status.TapeRecall
      if task_status.status_on_server == StatusOnServer.SUBMITTED:
        task_status.status = Status.InProgress
      if task_status.status_on_server == StatusOnServer.KILLED:
        task_status.status = Status.WaitingForRecovery
      if task_status.status_on_scheduler in [ StatusOnScheduler.FAILED, StatusOnScheduler.FAILED_KILLED ]:
        task_status.status = Status.WaitingForRecovery
      if task_status.status_on_scheduler == StatusOnScheduler.COMPLETED:
        task_status.status = Status.CrabFinished
      if task_status.status_on_server in [ StatusOnServer.SUBMITFAILED, StatusOnServer.RESUBMITFAILED ]:
        task_status.status = Status.WaitingForRecovery
    except RuntimeError as e:
      task_status.status = Status.Unknown
      task_status.parse_error = str(e)
    return task_status

  def sched_worker(task_status, log_lines, n, value):
    match = re.match(r'(.*) - (.*)', value)
    if match is None:
      raise RuntimeError("Invalid Grid scheduler - Task Worker")
    task_status.grid_scheduler = match.group(1)
    task_status.task_worker = match.group(2)
    return n + 1

  def status_on_server(task_status, log_lines, n, value):
    if value == "QUEUED on command SUBMIT":
      task_status.status_on_server = StatusOnServer.QUEUED
    else:
      if value not in StatusOnServer.__members__:
        raise RuntimeError(f'Unknown status on the CRAB server = "{value}"')
      task_status.status_on_server = StatusOnServer[value]
    return n + 1

  def status_on_scheduler(task_status, log_lines, n, value):
    if value == 'FAILED (KILLED)':
      task_status.status_on_scheduler = StatusOnScheduler.FAILED_KILLED
    else:
      if value not in StatusOnScheduler.__members__:
        raise RuntimeError(f'Unknown status on the scheduler = "{value}"')
      task_status.status_on_scheduler = StatusOnScheduler[value]
    return n + 1

  def crab_dev(task_status, log_lines, n, value):
    task_status.crab_dev = value
    n += 1
    if log_lines[n].strip() == "Be sure to have a good reason for using it":
      n += 1
    return n

  def warning(task_status, log_lines, n, value):
    warning_text = value
    while n < len(log_lines) - 1:
      n += 1
      line = log_lines[n].strip()
      if len(line) == 0 or log_lines[n][0] != ' ':
        break
      warning_text += f'\n{log_lines[n].strip()}'
    task_status.warnings.append(CrabWarning(warning_text))
    return n

  def failure(task_status, log_lines, n, value):
    text = value
    while n < len(log_lines) - 1:
      n += 1
      line = log_lines[n].strip()
      if len(line) == 0 or log_lines[n][0] != ' ':
        break
      text += f'\n{log_lines[n].strip()}'
    task_status.failure = CrabFailure(text)
    return n

  def job_status(task_status, log_lines, n, value):
    job_stat_strs = [ value ]
    n += 1
    while n < len(log_lines):
      line = log_lines[n].strip()
      if len(line) == 0: break
      job_stat_strs.append(line)
      n += 1
    for s in job_stat_strs:
      m = re.match(r'^([^ ]+) *([0-9\.]+)% *\( *([0-9]+)/([0-9]+)\)', s)
      if m is None:
        raise RuntimeError(f'can not extract job status from "{s}"')
      job_status_str = m.group(1)
      if job_status_str not in JobStatus.__members__:
        raise RuntimeError(f'Unknown job status = {job_status_str}')
      status = JobStatus[job_status_str]
      if status in task_status.job_stat:
        raise RuntimeError(f'Duplicated job status information for {status.name}')
      try:
        n_jobs = int(m.group(3))
        n_total = int(m.group(4))
      except ValueError:
        raise RuntimeError(f'Number of jobs is not an integer. "{s}"')
      task_status.job_stat[status] = n_jobs
      #fraction = float(m.group{2})

      if task_status.n_jobs_total is None:
        task_status.n_jobs_total = n_total
      if task_status.n_jobs_total != n_total:
        raise RuntimeError("Inconsistent total number of jobs")
    return n

  def error_summary(task_status, log_lines, n, value):
    error_stat_strs = []
    end_found = False
    while n < len(log_lines):
      n += 1
      line = log_lines[n].strip()
      if len(line) == 0:
        continue
      if line == LogEntryParser.error_summary_end:
        end_found = True
        break
    if not end_found:
      raise RuntimeError("Unable to find the end of the error summary")
    for stat_str in error_stat_strs:
      stat_str = stat_str.strip()
      match = re.match(r'([0-9]+) jobs failed with exit code ([0-9]+)', stat_str)
      if match is None:
        match = re.match(r'Could not find exit code details for [0-9]+ jobs.', stat_str)
        if match is None:
          raise RuntimeError(f'Unknown job summary string = "{stat_str}"')
        task_status.error_stat["Unknown"] = int(match.group(1))
      else:
        task_status.error_stat[int(match.group(2))] = int(match.group(1))
    return n + 1

  def run_summary(task_status, log_lines, n, value):
    if n + 4 >= len(log_lines):
      raise RuntimeError("Incomplete summary of run jobs")

    mem_str = log_lines[n + 1].strip()
    match = re.match(r'^\* Memory: ([0-9]+)MB min, ([0-9]+)MB max, ([0-9]+)MB ave$', mem_str)
    if match is None:
      raise RuntimeError(f'Invalid memory stat = "{mem_str}"')
    task_status.run_stat["Memory"] = {
      'min': int(match.group(1)),
      'max': int(match.group(2)),
      'ave': int(match.group(3)),
    }

    def to_seconds(hh_mm_ss):
      hh, mm, ss = [ int(s) for s in hh_mm_ss.split(':') ]
      return ((hh * 60) + mm) + ss

    runtime_str = log_lines[n + 2].strip()
    time_re = '-*([0-9]+:[0-9]+:[0-9]+)'
    runtime_re = f'^\* Runtime: {time_re} min, {time_re} max, {time_re} ave$'
    match = re.match(runtime_re, runtime_str)
    if match is None:
      raise RuntimeError(f'Invalid runtime stat = "{runtime_str}"')
    task_status.run_stat["Runtime"] = {
      'min': to_seconds(match.group(1)),
      'max': to_seconds(match.group(2)),
      'ave': to_seconds(match.group(3)),
    }

    shift = 3
    cpu_str = log_lines[n + shift].strip()
    match = re.match(r'^\* CPU eff: ([0-9]+)% min, ([0-9]+)% max, ([0-9]+)% ave$', cpu_str)
    if match is not None:
      # raise RuntimeError(f'Invalid CPU eff stat = "{cpu_str}"')
      task_status.run_stat["CPU"] = {
        'min': int(match.group(1)),
        'max': int(match.group(2)),
        'ave': int(match.group(3)),
      }
      shift += 1

    waste_str = log_lines[n + shift].strip()
    match = re.match(r'^\* Waste: ([0-9]+:[0-9]+:[0-9]+) \(([0-9]+)% of total\)$', waste_str)
    if match is not None:
      task_status.run_stat["CPU"] = {
        'time': to_seconds(match.group(1)),
        'fraction_of_total': int(match.group(2)),
      }
      shift += 1

    return n + shift

  def task_bootstrapped(task_status, log_lines, n, value):
    if n + 1 >= len(log_lines) or log_lines[n + 1].strip() != LogEntryParser.status_will_be_available:
      raise RuntimeError("Unexpected bootstrap message")
    task_status.status = Status.Bootstrapped
    return n + 2

  def bootstrap_failed(task_status, log_lines, n, value):
    task_status.status_on_scheduler = StatusOnScheduler.FAILED
    n += 1
    if log_lines[n].strip().startswith('Hold reason:'):
      n += 1
    return n

  def details(task_status, log_entries, n, value):
    task_status.details = json.loads(log_entries[n])
    return n + 1

  _parser_dict = {
    "BEWARE: this is the development version of CRAB Client.": crab_dev,
    "CRAB project directory:": "project_dir",
    "Task name:": "task_name",
    "Grid scheduler - Task Worker:": sched_worker,
    "Status on the CRAB server:": status_on_server,
    "Task URL to use for HELP:": "task_url",
    "Dashboard monitoring URL:": "dashboard_url",
    "Status on the scheduler:": status_on_scheduler,
    "Warning:": warning,
    "Jobs status:": job_status,
    "No publication information": None,
    "Error Summary:": error_summary,
    "Log file is": "crab_log_file",
    "Summary of run jobs:": run_summary,
    "Task bootstrapped": task_bootstrapped,
    "Failure message from server:": failure,
    "{": details,
    "The task failed to bootstrap on the Grid scheduler": bootstrap_failed,
  }
  error_summary_end = "Have a look at https://twiki.cern.ch/twiki/bin/viewauth/CMSPublic/JobExitCodes for a description of the exit codes."
  status_will_be_available = "Status information will be available within a few minutes"

class CrabTaskStatus:
  def __init__(self):
    self.status = Status.Unknown
    self.log_lines = None
    self.parse_error = None

    self.project_dir = None
    self.task_name = None
    self.grid_scheduler = None
    self.task_worker = None
    self.status_on_server = None
    self.task_url = None
    self.dashboard_url = None
    self.status_on_scheduler = None
    self.warnings = []
    self.failure = None
    self.n_jobs_total = None
    self.job_stat = {}
    self.error_stat = {}
    self.crab_log_file = None
    self.run_stat = {}
    self.details = {}

  _string_fields = [ 'log_lines', 'parse_error', 'project_dir', 'task_name', 'grid_scheduler', 'task_worker',
                     'task_url', 'dashboard_url', 'crab_log_file' ]
  _enum_fields = [ 'status', 'status_on_server', 'status_on_scheduler' ]
  _int_fields = [ 'n_jobs_total' ]

  def get_detailed_job_stat(self, field_name, job_status):
    jobs = {}
    for job_id, job_stat in self.details.items():
      status = JobStatus[job_stat["State"]]
      if status != job_status: continue
      if field_name not in job_stat:
        raise RuntimeError(f'Job field "{field_name}" not found.')
      jobs[job_id] = job_stat[field_name]
    return jobs

  def get_job_status(self):
    jobs = {}
    for job_id, job_stat in self.details.items():
      status = JobStatus[job_stat["State"]]
      jobs[job_id] = status
    return jobs

  def task_id(self):
    return self.task_name.split(':')[0]

  def to_json(self):
    result = { }
    for name in CrabTaskStatus._string_fields + CrabTaskStatus._enum_fields + CrabTaskStatus._int_fields:
      value = getattr(self, name)
      if value is None: continue
      v_type = type(value)
      if v_type in [str, int]:
        result[name] = value
      else:
        result[name] = value.name

    if len(self.warnings) > 0:
      warnings = []
      for warning in self.warnings:
        warnings.append({ 'category': warning.category.name, 'message': warning.message })
      result['warnings'] = warnings

    if self.failure is not None:
      result['failure'] = { 'category': self.failure.category.name, 'message': self.failure.message }

    if len(self.job_stat) > 0:
      result['job_stat'] = { key.name: value for key,value in self.job_stat.items() }

    if len(self.error_stat) > 0:
      result['error_stat'] = self.error_stat

    if len(self.run_stat) > 0:
      result['run_stat'] = self.run_stat

    if len(self.details) > 0:
      result['details'] = self.details

    return json.dumps(result, indent=2)

  @staticmethod
  def from_json(json_str):
    result = json.loads(json_str)
    task_status = CrabTaskStatus()

    for name in CrabTaskStatus._string_fields + CrabTaskStatus._int_fields:
      if name not in result: continue
      setattr(task_status, name, result[name])
    if 'status' in result:
      task_status.status = Status[result['status']]
    if 'status_on_server' in result:
      task_status.status_on_server = StatusOnServer[result['status_on_server']]
    if 'status_on_scheduler' in result:
      task_status.status_on_scheduler = StatusOnScheduler[result['status_on_scheduler']]

    for warning in result.get('warnings', []):
      task_status.warnings.append(CrabWarning(warning['message']))

    if 'failure' in result:
      task_status.failure = CrabFailure(result['failure']['message'])

    for name, stat in result.get('job_stat', {}).items():
      task_status.job_stat[JobStatus[name]] = stat

    if 'error_stat' in result:
      task_status.error_stat = result['error_stat']
    if 'run_stat' in result:
      task_status.run_stat = result['run_stat']
    if 'details' in result:
      task_status.details = result['details']

    return task_status

if __name__ == "__main__":
  import sys

  log_file = sys.argv[1]
  with open(log_file, 'r') as f:
    log_lines = f.readlines()
  taskStatus = LogEntryParser.Parse(log_lines)
  print(taskStatus.status)
  if taskStatus.status == Status.Unknown:
    print(taskStatus.parse_error)
  else:
    for status, n in taskStatus.job_stat.items():
      print(f'{status.name} {float(n)/taskStatus.n_jobs_total * 100:.1f}% ({n}/{taskStatus.n_jobs_total})')
  for warning in taskStatus.warnings:
    if warning.category == CrabWarningCategory.Unknown:
      print(f'Unknown warning\n-----\n{warning.message}\n-----\n')
  json_str = taskStatus.to_json()
  taskStatus2 = CrabTaskStatus.from_json(json_str)
  json_str2 = taskStatus2.to_json()
  if(json_str2 != json_str):
    print(json_str)
    raise RuntimeError("Problem with json save/load.")
