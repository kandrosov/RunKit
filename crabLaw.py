import copy
import datetime
import json
import law
import luigi
import os
import pathlib
import select
import shutil
import sys
import tempfile
import termios
import threading


from .law_customizations import HTCondorWorkflow
from .crabTask import Task as CrabTask
from .crabTaskStatus import Status
from .run_tools import ps_call, print_ts

cond = threading.Condition()

def update_kinit(verbose=0):
  if shutil.which('kinit'):
    ps_call(['kinit', '-R'], expected_return_codes=None, verbose=verbose)
  if shutil.which('aklog'):
    ps_call(['aklog'], expected_return_codes=None, verbose=verbose)

def update_kinit_thread():
  timeout = 60.0 * 60 # 1 hour
  cond.acquire()
  while not cond.wait(timeout):
    update_kinit(verbose=1)
  cond.release()

class LawTaskManager:
  def __init__(self, cfg_path, law_dir=None, proc_params=None):
    self.cfg_path = cfg_path
    self.law_dir = law_dir
    self.proc_params = proc_params
    if self.law_dir:
      self.law_task_dir = os.path.join(self.law_dir, self.proc_params['lawTask'])
      self.grid_jobs_file = os.path.join(self.law_task_dir, f'{self.proc_params["workflow"]}_jobs.json')
      self.grid_jobs_file_history = os.path.join(self.law_task_dir, f'{self.proc_params["workflow"]}_jobs_hist.json')
    if os.path.exists(cfg_path):
      with open(cfg_path, 'r') as f:
        self.cfg = json.load(f)
      self.has_updates = False
    else:
      self.cfg = []
      self.has_updates = True

  def add(self, task_work_area, task_grid_job_id, done_flag, failed_flag=None, ready_to_run=True):
    task_work_area = os.path.abspath(task_work_area)
    done_flag = os.path.abspath(done_flag)
    failed_flag = os.path.abspath(failed_flag) if failed_flag is not None else None
    existing_entry = self.find(task_work_area, task_grid_job_id)
    if existing_entry is not None:
      if existing_entry.get('done_flag') != done_flag or existing_entry.get('failed_flag') != failed_flag \
         or existing_entry.get('ready_to_run', True) != ready_to_run:
        existing_entry['done_flag'] = done_flag
        existing_entry['failed_flag'] = failed_flag
        existing_entry['ready_to_run'] = ready_to_run
        self.has_updates = True
      return

    branch_id = len(self.cfg)
    self.cfg.append({ 'branch_id': branch_id, 'task_work_area': task_work_area, 'task_grid_job_id': task_grid_job_id,
                      'done_flag': done_flag, 'failed_flag': failed_flag, 'ready_to_run': ready_to_run })
    self.has_updates = True

  def find(self, task_work_area, task_grid_job_id):
    task_work_area = os.path.abspath(task_work_area)
    task_grid_job_id = int(task_grid_job_id)
    for entry in self.cfg:
      if entry['task_work_area'] == task_work_area and entry['task_grid_job_id'] == task_grid_job_id:
        return entry
    return None

  def find_by_branch_id(self, branch_id):
    branch_id = int(branch_id)
    for entry in self.cfg:
      if entry['branch_id'] == branch_id:
        return entry
    return None

  def get_cfg(self):
    cfg_ext = []
    entry_jobs = {}
    for entry in self.cfg:
      entry_ext = copy.deepcopy(entry)
      entry_ext['dependencies'] = []
      task_work_area = entry_ext['task_work_area']
      task_grid_job_id = entry_ext['task_grid_job_id']
      if task_work_area not in entry_jobs:
        entry_jobs[task_work_area] = {}
      entry_jobs[task_work_area][task_grid_job_id] = entry_ext
      cfg_ext.append(entry_ext)
    for task_work_area, jobs in entry_jobs.items():
      for job_id, job_entry in jobs.items():
        if job_id < 0:
          if job_id == -1:
            for task_grid_job_id, entry in jobs.items():
              if task_grid_job_id >= 0:
                job_entry['dependencies'].append(entry['done_flag'])
          elif job_id+1 in jobs:
            job_entry['dependencies'].append(jobs[job_id+1]['done_flag'])
    return cfg_ext

  def select_branches(self, task_work_areas):
    selected_branches = []
    for task_work_area in task_work_areas:
      task_work_area = os.path.abspath(task_work_area)
      for entry in self.cfg:
        if entry['task_work_area'] == task_work_area:
          selected_branches.append(entry['branch_id'])
    return selected_branches

  def clean_branches(self, task_work_areas, dry_run=False):
    task_work_areas = [ os.path.abspath(task_work_area) for task_work_area in task_work_areas ]
    new_cfg = []
    to_remove = []
    has_updates = False
    for entry in self.cfg:
      if entry['task_work_area'] in task_work_areas:
        new_cfg.append(entry)
      else:
        has_updates = True
        to_remove.append(entry)
    if not dry_run and has_updates:
      self.cfg = new_cfg
      self.has_updates = True
    return to_remove

  def _save_safe(self, file, json_content):
    tmp_path = file + '.tmp'
    with open(tmp_path, 'w') as f:
      json.dump(json_content, f, indent=2)
    shutil.move(tmp_path, file)

  def save(self):
    if self.has_updates:
      self._save_safe(self.cfg_path, self.cfg)
      self.has_updates = False

  def update_grid_jobs(self, task_work_areas=None):
    if self.law_dir is None or not os.path.exists(self.grid_jobs_file):
      return
    with open(self.grid_jobs_file, 'r') as f:
      grid_jobs = json.load(f)
    has_history_updates = False
    if os.path.exists(self.grid_jobs_file_history):
      with open(self.grid_jobs_file_history, 'r') as f:
        grid_jobs_history = json.load(f)
    else:
      grid_jobs_history = {
        "jobs": {}, "unsubmitted_jobs": {}, "attempts": {}, "dashboard_config": {}, "tasks_per_job": 1
      }
      has_history_updates = True
    task_areas = None
    if task_work_areas is not None:
      task_areas = set(os.path.abspath(task_work_area) for task_work_area in task_work_areas)
    has_updates = False
    valid_jobs = set()
    for entry in self.cfg:
      if task_areas is not None and entry['task_work_area'] not in task_areas:
        continue
      branch_id = entry['branch_id']
      job_id = str(branch_id + 1)
      valid_jobs.add(job_id)
      if job_id not in grid_jobs["jobs"] and job_id not in grid_jobs["unsubmitted_jobs"]:
        if not os.path.exists(entry['done_flag']):
          if job_id in grid_jobs_history["jobs"]:
            grid_jobs["jobs"][job_id] = grid_jobs_history["jobs"][job_id]
            grid_jobs["attempts"][job_id] = grid_jobs_history["attempts"].get(job_id, 1)
          else:
            grid_jobs["unsubmitted_jobs"][job_id] = [ branch_id ]
          has_updates = True
    for col in [ "jobs", "unsubmitted_jobs" ]:
      jobs_to_remove = []
      for job_id in grid_jobs[col]:
        if job_id not in valid_jobs:
          jobs_to_remove.append(job_id)
        if job_id not in grid_jobs_history[col] or grid_jobs_history[col][job_id] != grid_jobs[col][job_id]:
          grid_jobs_history[col][job_id] = grid_jobs[col][job_id]
          if job_id in grid_jobs["attempts"]:
            grid_jobs_history["attempts"][job_id] = grid_jobs["attempts"][job_id]
          has_history_updates = True
      for job_id in jobs_to_remove:
        grid_jobs[col].pop(job_id)
        has_updates = True
    if has_updates:
      self._save_safe(self.grid_jobs_file, grid_jobs)
    if has_history_updates:
      self._save_safe(self.grid_jobs_file_history, grid_jobs_history)

  def run_law(self, work_area, last_update, update_interval, local_tasks, select_branches):
    n_cpus = self.proc_params.get('nCPU', 1)
    max_runime = self.proc_params.get('maxRuntime', 48.0)
    max_parallel_jobs = self.proc_params.get('maxParallelJobs', 1000)
    stop_date = last_update + datetime.timedelta(minutes=update_interval)
    stop_date_str = stop_date.strftime('%Y-%m-%dT%H%M%S')
    cmd = [ 'law', 'run', self.proc_params['lawTask'],
            '--workflow', self.proc_params['workflow'],
            '--bootstrap-path', self.proc_params['bootstrap'],
            '--work-area', work_area,
            '--sub-dir', self.law_dir,
            '--n-cpus', str(n_cpus),
            '--max-runtime', str(max_runime),
            '--parallel-jobs', str(max_parallel_jobs),
            '--stop-date', stop_date_str,
            '--transfer-logs',
    ]
    if 'requirements' in self.proc_params:
      cmd.extend(['--requirements', self.proc_params['requirements']])
    task_work_areas = None
    if select_branches:
      task_work_areas = []
      for task in local_tasks:
        task_work_areas.append(task.workArea)
      selected_branches = self.select_branches(task_work_areas)
      if len(selected_branches) == 0:
        raise RuntimeError("No branches are selected for local processing.")
      branches_str = ','.join([ str(branch) for branch in selected_branches ])
      cmd.extend(['--branches', branches_str])
    self.update_grid_jobs(task_work_areas=task_work_areas)
    ps_call(cmd, verbose=1)

  def clean_logs(self, max_n_logs):
    if max_n_logs < 0:
      raise ValueError("max_n_logs must be non-negative.")

    log_path = pathlib.Path(self.law_task_dir)
    if not log_path.exists() or not log_path.is_dir():
      return
    txt_files = list(log_path.glob('*.txt'))
    if len(txt_files) > max_n_logs:
      print(f"Cleaning up logs in {log_path}. Current number of logs = {len(txt_files)}."
            f"Keeping the most recent {max_n_logs} files...")
      txt_files.sort(key=lambda p: p.stat().st_mtime)
      for file_path in txt_files[:-max_n_logs]:
        file_path.unlink()
      print("Log cleanup done.")

class ProdTask(HTCondorWorkflow, law.LocalWorkflow):
  work_area = luigi.Parameter()
  stop_date = luigi.parameter.DateSecondParameter(default=datetime.datetime.max)

  def local_path(self, *path):
    return os.path.join(self.htcondor_output_directory().path, *path)

  def workflow_requires(self):
    return {}

  def requires(self):
    return {}

  def law_job_home(self):
    if 'LAW_JOB_HOME' in os.environ:
      return os.environ['LAW_JOB_HOME'], False
    os.makedirs(self.local_path(), exist_ok=True)
    return tempfile.mkdtemp(dir=self.local_path()), True

  def create_branch_map(self):
    task_list_path = os.path.join(self.work_area, 'law_tasks.json')
    task_manager = LawTaskManager(task_list_path)
    branches = {}
    for entry in task_manager.get_cfg():
      branches[entry['branch_id']] = (entry['task_work_area'], entry['task_grid_job_id'], entry['done_flag'], entry['dependencies'], entry.get('failed_flag'), entry.get('ready_to_run', True))
    return branches

  def output(self):
    work_area, grid_job_id, done_flag, dependencies, failed_flag, ready_to_run = self.branch_data
    if failed_flag is not None:
      failed_flag_target = law.LocalFileTarget(failed_flag)
      if failed_flag_target.exists():
        return failed_flag_target
    done_flag_target = law.LocalFileTarget(done_flag)
    wait_flag_target = law.LocalFileTarget(done_flag + '.wait')
    all_dependecies_exist = True
    for dependency in dependencies:
      if not os.path.exists(dependency):
        all_dependecies_exist = False
        break
    if not ready_to_run or not all_dependecies_exist:
      wait_flag_target.touch()
      return wait_flag_target
    return done_flag_target

  def run(self):
    thread = threading.Thread(target=update_kinit_thread)
    thread.start()
    job_home, remove_job_home = self.law_job_home()
    try:
      work_area, grid_job_id, done_flag, dependencies, failed_flag, ready_to_run = self.branch_data
      task = CrabTask.Load(workArea=work_area)
      if grid_job_id == -2:
        if task.taskStatus.status in [ Status.PostProcessingFinished, Status.Finished ]:
          task.removeCrabOutputs()
          self.output().touch()
      elif grid_job_id == -1:
        done = False
        if task.taskStatus.status in [ Status.CrabFinished, Status.PostProcessingFinished ]:
          try:
            if task.taskStatus.status == Status.CrabFinished:
              print(f'{task.name}: post-processing ...')
              task.postProcessOutputs(job_home)
            self.output().touch()
            done = True
          except Exception as e:
            print(f'{task.name}: error while post-processing: {e}')
        else:
          print(f"task {task.name} is not ready for post-processing")
        if not done:
          failed_flag = failed_flag if failed_flag is not None else task.getPostProcessingFaliedFlagFile()
          failed_flag_target = law.LocalFileTarget(failed_flag)
          failed_flag_target.touch()
      else:
        if grid_job_id in task.getGridJobs():
          print(f'Running {task.name} job_id = {grid_job_id}')
          result = task.runJobLocally(grid_job_id, job_home)
          print(f'Finished running {task.name} job_id = {grid_job_id}. result = {result}')
        else:
          print(f'job_id = {grid_job_id} is not found in {task.name}. considering it as finished')
          result = True
        state_str = 'finished' if result else 'failed'
        with self.output().open('w') as output:
          output.write(state_str)
    finally:
      if remove_job_home:
        shutil.rmtree(job_home)
      cond.acquire()
      cond.notify_all()
      cond.release()
      thread.join()

  def htcondor_poll_callback(self, poll_data):
    return self.poll_callback(poll_data)

  def poll_callback(self, poll_data):
    update_kinit(verbose=0)
    self.poll_counter = getattr(self, 'poll_counter', 0) + 1
    rlist, wlist, xlist = select.select([sys.stdin], [], [], 0.1)
    if rlist:
      termios.tcflush(sys.stdin, termios.TCIOFLUSH)
      timeout = 120 # seconds
      print_ts('Input from terminal is detected. Press return to stop polling, otherwise polling will'
               f' continue in {timeout} seconds...')
      rlist, wlist, xlist = select.select([sys.stdin], [], [], timeout)
      if rlist:
        termios.tcflush(sys.stdin, termios.TCIOFLUSH)
        return False
      print_ts(f'Polling resumed')
    return self.poll_counter <= 1 or datetime.datetime.now() < self.stop_date

  def control_output_postfix(self):
    return ""
