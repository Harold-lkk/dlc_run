import argparse
import os
import re
import subprocess
from pathlib import Path

user_names = os.listdir('/cpfs01/user/')
if len(user_names) == 1:
    HOME = f'/cpfs01/user/{user_names[0]}'
elif len(user_names) == 2:
    if 'liukuikun' in user_names[0]:
        HOME = f'/cpfs01/user/{user_names[1]}'
    elif 'liukuikun' in user_names[1]:
        HOME = f'/cpfs01/user/{user_names[0]}'
    else:
        HOME = None
else:
    HOME = None


def get_conda_envs(shell='zsh'):
    """Function to retrieve all conda environments."""
    try:
        cmds = f'{shell} -i -c  "conda env list"'
        result = subprocess.run(cmds,
                                capture_output=True,
                                text=True,
                                check=True,
                                shell=True)
        envs = []
        for line in result.stdout.split('\n'):
            if line and not line.startswith('#'):
                env_name = line.split()[0]
                envs.append(env_name)
        return envs
    except subprocess.SubprocessError as e:
        print(f'Failed to retrieve conda environments: {e}')
        return []


def validate_conda_env(value, shell='zsh'):
    """Validate the conda environment name against the list of available
    environments or check if it's a valid path."""
    envs = get_conda_envs(shell)
    if value in envs or Path(value).exists() or not value:
        return value
    raise argparse.ArgumentTypeError(
        f'{value} is not a valid conda environment. '
        f"Available: {', '.join(envs)}")


def parse_env_vars(env_vars):
    """Parse list of key=value strings into a dictionary, supporting both
    comma-separated and multiple inputs."""
    env_dict = {}
    for item in env_vars:
        pairs = item.split(';')
        for pair in pairs:
            if '=' in pair:
                key, value = pair.split('=', 1)
                env_dict[key.strip()] = value.strip()
    return env_dict


def main():
    parser = argparse.ArgumentParser(
        description='Run a job with the specified configuration using DLC.')
    parser.add_argument('--data-sources', help='User ID')
    parser.add_argument('--priority', type=int, default=1, help='priority')
    parser.add_argument('--workspace-id',
                        type=int,
                        default=5366,
                        help='Workspace ID')
    parser.add_argument('--resource-id',
                        type=str,
                        default='quota12hhgcm8cia',
                        help='Quota ID')
    parser.add_argument('--job-name',
                        '-J',
                        default='dlc_job',
                        help='Name of the job')

    parser.add_argument('--config',
                        '-c',
                        default=os.path.expanduser(f'{HOME}/.dlc/config'),
                        help='DLC configuration path')
    parser.add_argument('--dlc-path',
                        default='/cpfs01/shared/public/dlc',
                        help='DLC installation path')
    parser.add_argument('--kind',
                        type=str,
                        choices=[
                            'elasticbatchjob', 'flinkbatchjob', 'pytorchjob',
                            'mpijob', 'tfjob', 'xgboostjob'
                        ],
                        default='pytorchjob',
                        help='Kind of job')

    parser.add_argument('--worker-count',
                        type=int,
                        default=1,
                        help='Number of workers')
    parser.add_argument('--worker-gpu',
                        type=int,
                        default=8,
                        help='GPU count for single worker')
    parser.add_argument('--worker-cpu',
                        type=int,
                        default=120,
                        help='CPU count for single worker')
    parser.add_argument(
        '--worker-image',
        type=str,
        default=
        'pjlab-wulan-acr-registry-vpc.cn-wulanchabu.cr.aliyuncs.com/pjlab-eflops/liukuikun:cu121-ubuntu22-lkk-0513-rc6',
        help='Docker image for the worker')
    parser.add_argument('--worker-memory',
                        type=int,
                        default=800,
                        help='Memory in GB for single worker')
    parser.add_argument('--interactive',
                        action='store_true',
                        help='Whether to print out job status or not.')
    parser.add_argument('--home', default=HOME, help='The path for HOME')
    parser.add_argument('--shell',
                        type=str,
                        choices=['bash', 'zsh', 'none'],
                        default='zsh',
                        help='Shell to use for command execution')
    parser.add_argument('--conda-env',
                        type=str,
                        default='',
                        help='Conda environment to activate')
    parser.add_argument('--env',
                        '--environs',
                        action='append',
                        default=[],
                        help=('Additional environment variables, '
                              "either `--env 'KEY1=VALUE1;KEY2=VALUE2'` or "
                              '`--env KEY1=VALUE1 --env KEY2=VALUE2`'))

    parser.add_argument(
        'task_cmds',
        # required=True,
        nargs=argparse.REMAINDER,
        help='Command line to execute, similar to typing in the shell.')
    args = parser.parse_args()
    try:
        args.conda_env = validate_conda_env(args.conda_env, args.shell)
    except argparse.ArgumentTypeError as e:
        parser.error(e)

    assert args.home is not None, 'HOME is not set.'
    home_cmd = f'export HOME={args.home}'
    shell_cmd = (f'{args.shell} -c "source ~/.{args.shell}rc'
                 if args.shell != 'none' else '')
    conda_cmd = f'conda activate {args.conda_env}' if args.conda_env else ''
    env_dict = parse_env_vars(args.env)
    env_var_cmds = ' && '.join(
        [f'export {key}={value}' for key, value in env_dict.items()])

    env_cmds = [
        cmd for cmd in [home_cmd, env_var_cmds, shell_cmd, conda_cmd] if cmd
    ]
    full_env_cmd = ' && '.join(env_cmds)

    full_command = (f'{full_env_cmd} && cd {os.getcwd()} && '
                    f"{' '.join(args.task_cmds).strip()}")
    if shell_cmd:
        full_command += '"'

    dlc_command = [
        f'{args.dlc_path} submit {args.kind}',
        f'--config {args.config}',
        f'--name {args.job_name}',
        f'--priority {args.priority}',
        f'--resource_id {args.resource_id}',
        f'--data_sources {args.data_sources}',
        f'--workspace_id {args.workspace_id}',
        f'--workers {args.worker_count}',
        f'--worker_cpu {args.worker_cpu}',
        f'--worker_gpu {args.worker_gpu}',
        f'--worker_memory {args.worker_memory}Gi',
        f'--worker_image {args.worker_image}',
        f'--worker_shared_memory {args.worker_memory // 2}Gi',
        '--interactive' if args.interactive else '',
        f"--command '{full_command}'",
    ]

    dlc_full_command = ' '.join(filter(None, dlc_command))
    print(f'Executing command:\n{dlc_full_command}')
    subprocess.run(dlc_full_command, shell=True, check=True)


if __name__ == '__main__':
    main()
