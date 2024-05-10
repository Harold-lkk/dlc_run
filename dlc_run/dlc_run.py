import argparse
import os
import re
import subprocess
from pathlib import Path

HOME = f'/cpfs01/user/{os.getenv("USER")}'


def get_workspace_id(partition: str, config_path: str, dlc_path: str) -> str:
    """Extract the workspace ID for the specified partition using dlc
    command."""
    config_path = os.path.expanduser(config_path)
    dlc_path = os.path.expanduser(dlc_path)
    try:
        result = subprocess.run(
            [dlc_path, 'get', 'workspace', '-c', config_path],
            capture_output=True,
            text=True,
            check=True)
        regex = rf'\|\s*{re.escape(partition)}\s*\|\s*([\w]+)\s*\|'
        match = re.search(regex, result.stdout, re.MULTILINE)
        if match:
            return match.group(1).strip()
    except subprocess.SubprocessError as e:
        print(f'Error retrieving workspace ID: {e}')
        return None
    return None


def get_conda_envs():
    """Function to retrieve all conda environments."""
    try:
        result = subprocess.run(['conda', 'env', 'list'],
                                capture_output=True,
                                text=True,
                                check=True)
        envs = []
        for line in result.stdout.split('\n'):
            if line and not line.startswith('#'):
                env_name = line.split()[0]
                envs.append(env_name)
        return envs
    except subprocess.SubprocessError as e:
        print(f'Failed to retrieve conda environments: {e}')
        return []


def validate_conda_env(value):
    """Validate the conda environment name against the list of available
    environments or check if it's a valid path."""
    envs = get_conda_envs()
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
    parser.add_argument('--job-name',
                        '-J',
                        default='dlc_job',
                        help='Name of the job')
    parser.add_argument('--partition',
                        '-p',
                        default='llmit',
                        help='Partition for DLC job')
    parser.add_argument('--config',
                        '-c',
                        default=os.path.expanduser(f'{HOME}/.dlc/config'),
                        help='DLC configuration path')
    parser.add_argument('--dlc-path',
                        default='/cpfs01/shared/public/dlc',
                        help='DLC installation path')
    parser.add_argument('--kind',
                        type=str,
                        choices=['TFJob', 'BatchJob', 'PyTorchJob'],
                        default='PyTorchJob',
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
        default='master0:5000/eflops/yehaochen:tears-and-blood1',
        help='Docker image for the worker')
    parser.add_argument('--worker-memory',
                        type=int,
                        default=800,
                        help='Memory in GB for single worker')
    parser.add_argument('--interactive',
                        action='store_true',
                        help='Whether to print out job status or not.')
    parser.add_argument('--shell',
                        type=str,
                        choices=['bash', 'zsh', 'none'],
                        default='none',
                        help='Shell to use for command execution')
    parser.add_argument('--conda-env',
                        type=validate_conda_env,
                        default='',
                        help='Conda environment to activate')
    parser.add_argument('--proxy',
                        action='store_true',
                        help='Toggle proxy settings')
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

    workspace_id = get_workspace_id(args.partition, args.config, args.dlc_path)
    if not workspace_id:
        print('Failed to retrieve workspace ID.')
        exit(1)

    home_cmd = f'export HOME={HOME}'
    shell_cmd = (f'{args.shell} -c "source ~/.{args.shell}rc'
                 if args.shell != 'none' else '')
    conda_cmd = f'conda activate {args.conda_env}' if args.conda_env else ''
    if args.proxy:
        proxy_cmd = ('export http_proxy=http://58.34.83.134:31128 && '
                     'export https_proxy=http://58.34.83.134:31128')
    else:
        proxy_cmd = 'unset http_proxy;unset https_proxy'
    env_dict = parse_env_vars(args.env)
    env_var_cmds = ' && '.join(
        [f'export {key}={value}' for key, value in env_dict.items()])

    env_cmds = [
        cmd
        for cmd in [home_cmd, proxy_cmd, env_var_cmds, shell_cmd, conda_cmd]
        if cmd
    ]
    full_env_cmd = ' && '.join(env_cmds)

    full_command = (
        f'{full_env_cmd} && cd {os.getcwd()} && '
        f"{' '.join(args.task_cmds).strip()}") + '"' if shell_cmd else ''

    dlc_command = [
        f'{args.dlc_path} create job',
        f'--config {args.config}',
        f'--name {args.job_name}',
        f'--kind {args.kind}',
        f'--worker_count {args.worker_count}',
        f'--worker_cpu {args.worker_cpu}',
        f'--worker_gpu {args.worker_gpu}',
        f'--worker_memory {args.worker_memory}',
        f'--worker_image {args.worker_image}',
        f'--workspace_id {workspace_id}',
        f'--worker_shared_memory {args.worker_memory // 2}',
        '--interactive' if args.interactive else '',
        f"--command '{full_command}'",
    ]

    dlc_full_command = ' '.join(filter(None, dlc_command))
    print(f'Executing command:\n{dlc_full_command}')
    subprocess.run(dlc_full_command, shell=True, check=True)


if __name__ == '__main__':
    main()
