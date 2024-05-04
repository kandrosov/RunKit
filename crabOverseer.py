import datetime
import json
import os
import select
import shutil
import sys
import yaml

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .crabTaskStatus import JobStatus, Status
from .crabTask import Task
from .crabLaw import LawTaskManager
from .run_tools import PsCallError, ps_call, print_ts, timestamp_str
from .grid_tools import get_voms_proxy_info, gfal_copy_safe, path_to_pfn, gfal_rm, gfal_exists

class TaskStat:
  summary_only_thr = 10

  def __init__(self):
    self.all_tasks = []
    self.tasks_by_status = {}
    self.n_jobs = 0
    self.total_job_stat = {}
    self.max_job_stat = {}
    self.unknown = []
    self.waiting_for_recovery = []
    self.failed = []
    self.tape_recall = []
    self.max_inactivity = None
    self.n_files_total = 0
    self.n_files_to_process = 0
    self.n_files_processed = 0
    self.n_files_ignored = 0
    self.status = { "lastUpdate": "", "tasks": [] }

  def add(self, task):
    self.all_tasks.append(task)

    useCacheOnly = task.taskStatus.status in [ Status.CrabFinished, Status.PostProcessingFinished, Status.Failed ]
    n_files_total, n_files_processed, n_files_to_process, n_files_ignored = \
      task.getFilesStats(useCacheOnly=useCacheOnly)
    self.n_files_total += n_files_total
    self.n_files_to_process += n_files_to_process
    self.n_files_processed += n_files_processed
    self.n_files_ignored += n_files_ignored
    self.status["tasks"].append({
      "name": task.name,
      "status": task.taskStatus.status.name,
      "recoveryIndex": task.recoveryIndex,
      "n_files": n_files_total,
      "n_processed": n_files_processed,
      "n_to_process": n_files_to_process,
      "n_ignored": n_files_ignored,
      "grafana": task.taskStatus.dashboard_url,
    })

    if task.taskStatus.status not in self.tasks_by_status:
      self.tasks_by_status[task.taskStatus.status] = []
    self.tasks_by_status[task.taskStatus.status].append(task)
    if task.taskStatus.status == Status.InProgress:
      for job_status, count in task.taskStatus.job_stat.items():
        if job_status not in self.total_job_stat:
          self.total_job_stat[job_status] = 0
        self.total_job_stat[job_status] += count
        self.n_jobs += count
        if job_status not in self.max_job_stat or self.max_job_stat[job_status][0] < count:
          self.max_job_stat[job_status] = (count, task)
      delta_t = int(task.getTimeSinceLastJobStatusUpdate())
      if delta_t > 0 and (self.max_inactivity is None or delta_t > self.max_inactivity[1]):
        self.max_inactivity = (task, delta_t)
    if task.taskStatus.status == Status.Unknown:
      self.unknown.append(task)
    if task.taskStatus.status == Status.WaitingForRecovery:
      self.waiting_for_recovery.append(task)
    if task.taskStatus.status == Status.Failed:
      self.failed.append(task)
    if task.taskStatus.status == Status.TapeRecall:
      self.tape_recall.append(task)


  def report(self):
    status_list = sorted(self.tasks_by_status.keys(), key=lambda x: x.value)
    n_tasks = len(self.all_tasks)
    status_list = [ f"{n_tasks} Total" ] + [ f"{len(self.tasks_by_status[x])} {x.name}" for x in status_list ]
    status_list_str = 'Tasks: ' + ', '.join(status_list)
    self.status["tasksSummary"] = status_list_str
    print(status_list_str)
    job_stat = [ f"{self.n_jobs} total" ] + \
               [ f'{cnt} {x.name}' for x, cnt in sorted(self.total_job_stat.items(), key=lambda a: a[0].value) ]
    job_stat_str = 'Jobs in active tasks: ' + ', '.join(job_stat)
    self.status["jobsSummary"] = job_stat_str
    if self.n_jobs > 0:
      print(job_stat_str)
    print(f'Input files: {self.n_files_total} total, {self.n_files_processed} processed,'
          f' {self.n_files_to_process} to_process, {self.n_files_ignored} ignored')
    if Status.InProgress in self.tasks_by_status:
      if len(self.tasks_by_status[Status.InProgress]) > TaskStat.summary_only_thr:
        if(len(self.max_job_stat.items())):
          print('Task with ...')
          for job_status, (cnt, task) in sorted(self.max_job_stat.items(), key=lambda a: a[0].value):
            print(f'\tmax {job_status.name} jobs = {cnt}: {task.name} {task.taskStatus.dashboard_url}')
          if self.max_inactivity is not None:
            task, delta_t = self.max_inactivity
            print(f'\tmax since_last_job_status_change = {delta_t}h: {task.name} {task.taskStatus.dashboard_url}')
      else:
        for task in self.tasks_by_status[Status.InProgress]:
          text = f'{task.name}: status={task.taskStatus.status.name}. '
          delta_t = int(task.getTimeSinceLastJobStatusUpdate())
          if delta_t > 0:
            text += f' since_last_job_status_change={delta_t}h. '

          job_info = []
          for job_status, count in sorted(task.taskStatus.job_stat.items(), key=lambda x: x[0].value):
            job_info.append(f'{count} {job_status.name}')
          if len(job_info) > 0:
            text += ', '.join(job_info) + '. '
          if task.taskStatus.dashboard_url is not None:
            text += task.taskStatus.dashboard_url
          print(text)
    if len(self.unknown) > 0:
      print('Tasks with unknown status:')
      for task in self.unknown:
        print(f'{task.name}: {task.taskStatus.parse_error}. {task.lastCrabStatusLog()}')
    if len(self.waiting_for_recovery) > 0:
      names = [ task.name for task in self.waiting_for_recovery ]
      print(f"Tasks waiting for recovery: {', '.join(names)}")
    if len(self.tape_recall) > 0:
      names = [ task.name for task in self.tape_recall ]
      print(f"Tasks waiting for a tape recall to complete: {', '.join(names)}")
    if len(self.failed) > 0:
      names = [ task.name for task in self.failed ]
      print(f"Failed tasks that require manual intervention: {', '.join(names)}")

def sanity_checks(task):
  abnormal_inactivity_thr = task.getMaxJobRuntime() + 1

  if task.taskStatus.status in [ Status.InProgress, Status.Submitted ]:
    delta_t = task.getTimeSinceLastJobStatusUpdate()
    if delta_t > abnormal_inactivity_thr:
      text = f'{task.name}: status of all jobs is not changed for at least {delta_t:.1f} hours.' \
              + ' It is very likely that this task is stacked. The task will be killed following by recovery attempts.'
      print(text)
      task.kill()
      return False

  if task.taskStatus.status == Status.InProgress:
    job_states = sorted(task.taskStatus.job_stat.keys(), key=lambda x: x.value)
    ref_states = [ JobStatus.running, JobStatus.finished, JobStatus.failed ]
    if len(job_states) <= len(ref_states) and job_states == ref_states[:len(job_states)]:
      now = datetime.datetime.now()
      start_times = task.taskStatus.get_detailed_job_stat('StartTimes', JobStatus.running)

      job_runs = []
      for job_id, start_time in start_times.items():
        t = datetime.datetime.fromtimestamp(start_time[-1])
        delta_t = (now - t).total_seconds() / (60 * 60)
        job_runs.append([job_id, delta_t])
      job_runs = sorted(job_runs, key=lambda x: x[1])
      max_run = job_runs[0][1]
      if max_run > abnormal_inactivity_thr:
        text = f'{task.name}: all running jobs are running for at least {max_run:.1f} hours.' \
              + ' It is very likely that these jobs are stacked. Task will be killed following by recovery attempts.'
        print(text)
        task.kill()
        return False

  return True

def update(tasks, lawTaskManager, maxNumberOfActiveCrabTasks, no_status_update=False):
  print_ts("Updating...")
  stat = TaskStat()
  to_post_process = []
  to_run_locally = []
  to_submit = []
  to_recover = []
  for task_name, task in tasks.items():
    if task.taskStatus.status == Status.Defined:
      to_submit.append(task)
    elif task.taskStatus.status.value < Status.CrabFinished.value:
      if task.taskStatus.status.value < Status.WaitingForRecovery.value and not no_status_update:
        if task.updateStatus(lawTaskManager=lawTaskManager):
          to_run_locally.append(task)
      if task.taskStatus.status == Status.WaitingForRecovery:
        to_recover.append(task)
    sanity_checks(task)
    if task.taskStatus.status == Status.CrabFinished:
      if task.checkCompleteness():
        done_flag = task.getPostProcessingDoneFlagFile()
        if os.path.exists(done_flag):
          task.taskStatus.status = Status.PostProcessingFinished
          task.endDate = timestamp_str()
          task.saveStatus()
          task.saveCfg()
          print(f'{task.name}: post-processing is done.')
          task.removeProcessedFiles()
        else:
          failed_flag = task.getPostProcessingFaliedFlagFile()
          if os.path.exists(failed_flag):
            print(f'{task.name}: post-processing failed. Checking consistency of processed files...')
            if not task.checkProcessedFiles(lawTaskManager=lawTaskManager, resetStatus=True):
              to_recover.append(task)
              lawTaskManager.add(task.workArea, -1, done_flag, failed_flag=failed_flag, ready_to_run=False)
            os.remove(failed_flag)
          else:
            lawTaskManager.add(task.workArea, -1, done_flag, failed_flag=failed_flag)
            to_post_process.append(task)
      else:
        task.taskStatus.status = Status.WaitingForRecovery
        task.saveStatus()
        to_recover.append(task)
  nActiveCrabTasks = 0
  for task_name, task in tasks.items():
    if not task.isInLocalRunMode() and task.taskStatus.status.value < Status.WaitingForRecovery.value:
      nActiveCrabTasks += 1
  to_act = [ (task, 'recover') for task in to_recover ] + [ (task, 'submit') for task in to_submit ]
  for task, action in to_act:
    allowCrabAction = nActiveCrabTasks < maxNumberOfActiveCrabTasks
    need_local_run, crab_task_submitted = getattr(task, action)(lawTaskManager=lawTaskManager,
                                                                allowCrabAction=allowCrabAction)
    if need_local_run:
      to_run_locally.append(task)
    if crab_task_submitted:
      nActiveCrabTasks += 1
  if nActiveCrabTasks >= maxNumberOfActiveCrabTasks:
    print(f'No new crab tasks will be submitted before the number of active crab tasks decreases below {maxNumberOfActiveCrabTasks}.'
          f' The current number of active crab tasks = {nActiveCrabTasks}.')
  for task_name, task in tasks.items():
    stat.add(task)
  stat.report()
  stat.status["lastUpdate"] = timestamp_str()
  lawTaskManager.save()
  return to_post_process, to_run_locally, stat.status

def apply_action(action, tasks, selected_tasks, task_list_path, lawTaskManager, vomsToken):
  if action == 'print':
    for task_name, task in selected_tasks.items():
      print(task.name)
  elif action.startswith('run_cmd'):
    cmd = action[len('run_cmd') + 1:]
    for task_name, task in selected_tasks.items():
      exec(cmd)
      task.saveCfg()
      task.saveStatus()
  elif action == 'list_files_to_process':
    for task_name, task in selected_tasks.items():
      print(f'{task.name}: files to process')
      for file in task.getFilesToProcess():
        print(f'  {file}')
  elif action in ['check_failed', 'check_update_failed']:
    print('Checking files availability for failed tasks...')
    resetStatus = action == 'check_update_failed'
    for task_name, task in selected_tasks.items():
      if task.taskStatus.status == Status.Failed:
        task.checkFilesToProcess(lawTaskManager=lawTaskManager, resetStatus=resetStatus)
    if resetStatus:
      lawTaskManager.save()
  elif action == 'ignore_failed':
    print('Ignoring missing files in failed tasks...')
    for task_name, task in selected_tasks.items():
      if task.taskStatus.status == Status.Failed:
        task.ignoreMissingFiles(lawTaskManager=lawTaskManager)
  elif action in ['check_processed', 'check_update_processed']:
    print('Checking output files for finished but not yet post-processed tasks...')
    resetStatus = action == 'check_update_processed'
    for task_name, task in selected_tasks.items():
      if task.taskStatus.status == Status.CrabFinished:
        task.checkProcessedFiles(lawTaskManager=lawTaskManager, resetStatus=resetStatus)
    if resetStatus:
      lawTaskManager.save()
  elif action == 'reset_local_jobs':
    for task_name, task in selected_tasks.items():
      if task.taskStatus.status in [ Status.Failed, Status.SubmittedToLocal ]:
        print(f'{task.name}: resetting local jobs...')
        task.gridJobs = None
        for file in [ task.gridJobsFile(), task.getGridJobDoneFlagDir() ]:
          if os.path.exists(file):
            if os.path.isfile(file):
              os.remove(file)
            else:
              shutil.rmtree(file)
        task.recoveryIndex = task.maxRecoveryCount
        task.taskStatus.status = Status.SubmittedToLocal
        task.saveStatus()
        task.saveCfg()
  elif action == 'remove_crab_output':
    for task_name, task in selected_tasks.items():
      task.removeProcessedFiles()
  elif action == 'kill':
    for task_name, task in selected_tasks.items():
      print(f'{task.name}: sending kill request...')
      try:
        task.kill()
      except PsCallError as e:
        print(f'{task.name}: error sending kill request. {e}')
  elif action == 'remove':
    for task_name, task in selected_tasks.items():
      print(f'{task.name}: removing...')
      shutil.rmtree(task.workArea)
      del tasks[task.name]
    with open(task_list_path, 'w') as f:
      json.dump([task_name for task_name in tasks], f, indent=2)
  else:
    raise RuntimeError(f'Unknown action = "{action}"')

def check_prerequisites(main_cfg):
  voms_info = get_voms_proxy_info()
  if 'timeleft' not in voms_info or voms_info['timeleft'] < 1:
    raise RuntimeError('Voms proxy is not initalised or is going to expire soon.' + \
                       ' Please run "voms-proxy-init -voms cms -rfc -valid 192:00".')
  if 'localProcessing' not in main_cfg or 'LAW_HOME' not in os.environ:
    raise RuntimeError("Law environment is not setup. It is needed to run the local processing step.")

def load_tasks(work_area, task_list_path, new_task_list_files, main_cfg, update_cfg, task_selection,
               task_selected_names, task_selected_status):
  tasks = {}
  created_tasks = set()
  updated_tasks = set()
  if os.path.isfile(task_list_path):
    with open(task_list_path, 'r') as f:
      task_names = json.load(f)
      for task_name in task_names:
        tasks[task_name] = Task.Load(mainWorkArea=work_area, taskName=task_name)
  if len(new_task_list_files) > 0:
    task_list_changed = False
    for task_list_file in new_task_list_files:
      with open(task_list_file, 'r') as f:
        new_tasks = yaml.safe_load(f)
      for task_name in new_tasks:
        if task_name == 'config': continue
        if task_selected_names is not None and task_name not in task_selected_names:
          continue
        if task_name in tasks:
          if update_cfg:
            tasks[task_name].updateConfig(main_cfg, new_tasks)
            updated_tasks.add(task_name)
        else:
          tasks[task_name] = Task.Create(work_area, main_cfg, new_tasks, task_name)
          created_tasks.add(task_name)
          task_list_changed = True
    if task_list_changed:
      with open(task_list_path, 'w') as f:
        json.dump(list(sorted(tasks.keys())), f, indent=2)
  if len(created_tasks) > 0:
    print(f'Created tasks: {", ".join(created_tasks)}')
  if update_cfg:
    if len(updated_tasks) > 0:
      print(f'Configuration updated for tasks: {", ".join(updated_tasks)}')
    not_updated = set(tasks.keys()) - created_tasks - updated_tasks
    if len(not_updated) > 0:
      print(f'Configuration not updated for tasks: {", ".join(not_updated)}')

  selected_tasks = {}
  for task_name, task in tasks.items():
    pass_selection = task_selection is None or eval(task_selection)
    pass_name_selection = task_selected_names is None or task.name in task_selected_names
    pass_status_selection = task_selected_status is None or task.taskStatus.status in task_selected_status
    if pass_selection and pass_name_selection and pass_status_selection:
      selected_tasks[task_name] = task

  return tasks, selected_tasks

def overseer_main(work_area, cfg_file, new_task_list_files, verbose=1, no_status_update=False,
                  update_cfg=False, no_loop=False, task_selection=None, task_selected_names=None,
                  task_selected_status=None, action=None):
  if not os.path.exists(work_area):
    os.makedirs(work_area)
  abs_work_area = os.path.abspath(work_area)
  cfg_path = os.path.join(work_area, 'cfg.yaml')
  if cfg_file is not None:
    shutil.copyfile(cfg_file, cfg_path)
  if not os.path.isfile(cfg_path):
    raise RuntimeError("The overseer configuration is not found")
  with open(cfg_path, 'r') as f:
    main_cfg = yaml.safe_load(f)

  check_prerequisites(main_cfg)

  task_list_path = os.path.join(work_area, 'tasks.json')
  all_tasks, selected_tasks = load_tasks(work_area, task_list_path, new_task_list_files, main_cfg, update_cfg,
                                         task_selection, task_selected_names, task_selected_status)

  lawTaskManager = LawTaskManager(os.path.join(work_area, 'law_tasks.json'))
  vomsToken = get_voms_proxy_info()['path']

  for name, task in selected_tasks.items():
    task.checkConfigurationValidity()

  if action is not None:
    apply_action(action, all_tasks, selected_tasks, task_list_path, lawTaskManager, vomsToken)
    return

  tasks = selected_tasks
  update_interval = main_cfg.get('updateInterval', 60)
  htmlUpdated = False

  while True:
    last_update = datetime.datetime.now()
    to_post_process, to_run_locally, status = update(tasks, lawTaskManager,
                                                     main_cfg.get('maxNumberOfActiveCrabTasks', 100),
                                                     no_status_update=no_status_update)

    status_path = os.path.join(work_area, 'status.json')
    with(open(status_path, 'w')) as f:
      json.dump(status, f, indent=2)
    htmlReportDest = main_cfg.get('htmlReport', '')
    if len(htmlReportDest) > 0:
      htmlReportDest = path_to_pfn(htmlReportDest)
      file_dir = os.path.dirname(os.path.abspath(__file__))
      filesToCopy = [ status_path ]
      if not htmlUpdated:
        for file in [ 'index.html', 'jquery.min.js', 'jsgrid.css', 'jsgrid.min.js', 'jsgrid-theme.css']:
          filesToCopy.append(os.path.join(file_dir, 'html', file))
      for file in filesToCopy:
        _, fileName = os.path.split(file)
        dest = os.path.join(htmlReportDest, fileName)
        gfal_copy_safe(file, dest, voms_token=vomsToken, verbose=0)
      print(f'HTML report is updated in {htmlReportDest}.')
      htmlUpdated = True

    if len(to_run_locally) > 0 or len(to_post_process) > 0:
      if len(to_run_locally) > 0:
        print_ts("To run on local grid: " + ', '.join([ task.name for task in to_run_locally ]))
      if len(to_post_process) > 0:
        print_ts("Post-processing: " + ', '.join([ task.name for task in to_post_process ]))
      print_ts(f"Total number of jobs to run on a local grid: {len(lawTaskManager.cfg)}")
      local_proc_params = main_cfg['localProcessing']
      law_sub_dir = os.path.join(abs_work_area, 'law', 'jobs')
      law_jobs_cfg = os.path.join(law_sub_dir, local_proc_params['lawTask'],
                                  f'{local_proc_params["workflow"]}_jobs.json')

      lawTaskManager.update_grid_jobs(law_jobs_cfg)
      n_cpus = local_proc_params.get('nCPU', 1)
      max_runime = local_proc_params.get('maxRuntime', 24.0)
      max_parallel_jobs = local_proc_params.get('maxParallelJobs', 1000)
      stop_date = last_update + datetime.timedelta(minutes=update_interval)
      stop_date_str = stop_date.strftime('%Y-%m-%dT%H%M%S')
      cmd = [ 'law', 'run', local_proc_params['lawTask'],
              '--workflow', local_proc_params['workflow'],
              '--bootstrap-path', local_proc_params['bootstrap'],
              '--work-area', abs_work_area,
              '--sub-dir', law_sub_dir,
              '--n-cpus', str(n_cpus),
              '--max-runtime', str(max_runime),
              '--parallel-jobs', str(max_parallel_jobs),
              '--stop-date', stop_date_str,
              '--transfer-logs',
      ]
      if 'requirements' in local_proc_params:
        cmd.extend(['--requirements', local_proc_params['requirements']])
      if len(all_tasks) != len(tasks):
        task_work_areas = []
        for task in to_run_locally + to_post_process:
          task_work_areas.append(task.workArea)
        selected_branches = lawTaskManager.select_branches(task_work_areas)
        if len(selected_branches) == 0:
          raise RuntimeError("No branches are selected for local processing.")
        branches_str = ','.join([ str(branch) for branch in selected_branches ])
        cmd.extend(['--branches', branches_str])
      ps_call(cmd)
      for task in to_post_process + to_run_locally:
        task.updateStatusFromFile()
      print_ts("Local grid processing iteration finished.")
    has_unfinished = False
    for task_name, task in tasks.items():
      if task.taskStatus.status not in [ Status.PostProcessingFinished, Status.Failed ]:
        has_unfinished = True
        break

    if no_loop or not has_unfinished: break
    delta_t = (datetime.datetime.now() - last_update).total_seconds() / 60
    to_sleep = int(update_interval - delta_t)
    if to_sleep >= 1:
      print_ts(f"Waiting for {to_sleep} minutes until the next update. Press return to exit.", prefix='\n')
      rlist, wlist, xlist = select.select([sys.stdin], [], [], to_sleep * 60)
      if rlist:
        print_ts("Exiting...")
        break
    if main_cfg.get('renewKerberosTicket', False):
      ps_call(['kinit', '-R'])
  if not has_unfinished:
    print("All tasks are done.")

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='CRAB overseer.')
  parser.add_argument('--work-area', required=False, type=str, default='.crabOverseer',
                      help="Working area to store overseer and crab tasks states")
  parser.add_argument('--cfg', required=False, type=str, default=None, help="configuration file")
  parser.add_argument('--no-status-update', action="store_true", help="Do not update tasks statuses.")
  parser.add_argument('--update-cfg', action="store_true", help="Update task configs.")
  parser.add_argument('--no-loop', action="store_true", help="Run task update once and exit.")
  parser.add_argument('--select', required=False, type=str, default=None,
                      help="select tasks using python expression. Default: select all.")
  parser.add_argument('--select-names', required=False, type=str, default=None,
                      help="select tasks with given names (use a comma separated list for multiple names)")
  parser.add_argument('--select-names-from-file', required=False, type=str, default=None,
                      help="select tasks with given names from a json file")
  parser.add_argument('--select-status', required=False, type=str, default=None,
                      help="select tasks with given status (use a comma separated list for multiple status)")
  parser.add_argument('--action', required=False, type=str, default=None,
                      help="apply action on selected tasks and exit")
  parser.add_argument('--verbose', required=False, type=int, default=1, help="verbosity level")
  parser.add_argument('task_file', type=str, nargs='*', help="file(s) with task descriptions")
  args = parser.parse_args()

  apply_selection = False
  selected_names = set()
  if args.select_names is not None:
    apply_selection = True
    for name in args.select_names.split(','):
      selected_names.add(name)
  if args.select_names_from_file is not None:
    apply_selection = True
    with open(args.select_names_from_file, 'r') as f:
      for name in json.load(f):
        selected_names.add(name)
  selected_names = list(selected_names) if apply_selection else None
  selected_status = None
  if args.select_status is not None:
    selected_status = set()
    for status in args.select_status.split(','):
      selected_status.add(Status[status])

  overseer_main(args.work_area, args.cfg, args.task_file, verbose=args.verbose, no_status_update=args.no_status_update,
                update_cfg=args.update_cfg, no_loop=args.no_loop, task_selection=args.select,
                task_selected_names=selected_names, task_selected_status=selected_status, action=args.action)
